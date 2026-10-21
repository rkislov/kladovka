// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/auth"
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/storage"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestVersionedLegacyReplicationRestoreAndRetention(t *testing.T) {
	servers := []*httptest.Server{httptest.NewUnstartedServer(nil), httptest.NewUnstartedServer(nil)}
	nodes := make([]*Server, 2)
	for i, ts := range servers {
		self := "http://" + ts.Listener.Addr().String()
		peer := "http://" + servers[1-i].Listener.Addr().String()
		st, e := storage.NewWithOptions(t.TempDir(), storage.Options{Versioning: true, VerifyReads: true})
		if e != nil {
			t.Fatal(e)
		}
		cfg := &config.Config{VersioningEnabled: true, TrashRetention: time.Hour, AccessKey: "test", SecretKey: "secret", Region: "us-east-1", NodeID: cluster.NodeIDFromURL(self), NodeURL: self, ReplicationFactor: 2, ClusterToken: "cluster-secret"}
		reg := cluster.NewRegistry(cfg.NodeID, self, []string{peer}, true, time.Second)
		reg.SetToken(cfg.ClusterToken)
		s := &Server{Cfg: cfg, Store: st, Reg: reg, Rep: &cluster.Replicator{Registry: reg, ReplicationFactor: 2}, Auth: &auth.Authenticator{Region: cfg.Region, Creds: auth.Credentials{"test": "secret"}}}
		nodes[i] = s
		ts.Config.Handler = s.Handler()
		ts.Start()
		t.Cleanup(ts.Close)
	}
	request := func(i int, method, path string, body []byte) (int, http.Header, []byte) {
		t.Helper()
		r, _ := http.NewRequest(method, servers[i].URL+path, bytes.NewReader(body))
		r.SetBasicAuth("test", "secret")
		res, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		data, _ := io.ReadAll(res.Body)
		return res.StatusCode, res.Header, data
	}
	if code, _, b := request(0, "POST", "/api/polki", []byte(`{"name":"docs"}`)); code != 201 {
		t.Fatal(code, string(b))
	}
	code, h, b := request(0, "PUT", "/api/object/docs/x", []byte("first"))
	if code != 200 {
		t.Fatal(code, string(b))
	}
	first := h.Get("X-Amz-Version-Id")
	if first == "" {
		t.Fatal("missing version")
	}
	if code, _, b = request(1, "PUT", "/api/object/docs/x", []byte("second")); code != 200 {
		t.Fatal(code, string(b))
	}
	code, h, b = request(1, "DELETE", "/api/object/docs/x", nil)
	if code != 204 {
		t.Fatal(code, string(b))
	}
	marker := h.Get("X-Amz-Version-Id")
	for _, n := range nodes {
		list, e := n.versions().ListVersions("docs", "")
		if e != nil || len(list) != 3 || list[0].VersionID != marker || !list[0].DeleteMarker {
			t.Fatal(list, e)
		}
		r, _, e := n.versions().GetVersion("docs", "x", first)
		if e != nil {
			t.Fatal(e)
		}
		data, _ := io.ReadAll(r)
		r.Close()
		if string(data) != "first" {
			t.Fatal(string(data))
		}
	}
	payload, _ := json.Marshal(map[string]string{"action": "restore", "version_id": first})
	code, _, b = request(1, "POST", "/api/versions/docs/x", payload)
	if code != 200 {
		t.Fatal(code, string(b))
	}
	for i := range nodes {
		code, _, b = request(i, "GET", "/api/object/docs/x", nil)
		if code != 200 || string(b) != "first" {
			t.Fatal(code, string(b))
		}
	}
	code, h, b = request(0, "DELETE", "/api/object/docs/x", nil)
	if code != 204 {
		t.Fatal(code, string(b))
	}
	now := time.Now()
	for _, n := range nodes {
		if e := n.CleanupTrash(context.Background(), now); e != nil {
			t.Fatal(e)
		}
	}
	if list, _ := nodes[0].versions().ListVersions("docs", ""); len(list) != 5 {
		t.Fatal("premature expiry", list)
	}
	for _, n := range nodes {
		n.Cfg.TrashRetention = 0
		if e := n.CleanupTrash(context.Background(), now.Add(48*time.Hour)); e != nil {
			t.Fatal(e)
		}
	}
	if list, _ := nodes[0].versions().ListVersions("docs", ""); len(list) != 5 {
		t.Fatal("disabled cleanup removed history")
	}
	for _, n := range nodes {
		n.Cfg.TrashRetention = time.Hour
		if e := n.CleanupTrash(context.Background(), now.Add(2*time.Hour)); e != nil {
			t.Fatal(e)
		}
	}
	for _, n := range nodes {
		if list, e := n.versions().ListVersions("docs", ""); e != nil || len(list) != 0 {
			t.Fatal("replica expiry diverged", list, e)
		}
	}
}
func TestSignedVersionListAndRead(t *testing.T) {
	s, _ := testServer(t, 100)
	st, e := storage.NewWithOptions(t.TempDir(), storage.Options{Versioning: true})
	if e != nil {
		t.Fatal(e)
	}
	s.Store = st
	s.Cfg.VersioningEnabled = true
	st.CreatePolka("docs")
	v, _ := st.PutObject("docs", "x", bytes.NewBufferString("first"), "")
	st.TrashObject("docs", "x")
	h := s.Handler()
	for _, path := range []string{"/docs?versions&max-keys=1", "/docs/x?versionId=" + v.VersionID, "/docs?versioning"} {
		r := httptest.NewRequest("GET", path, nil)
		sign(r, nil, time.Now())
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}

func TestVersionDeletionRequiresAdminAndCleanupRequiresAudit(t *testing.T) {
	s, _ := testServer(t, 1024)
	st, err := storage.NewWithOptions(t.TempDir(), storage.Options{Versioning: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Store = st
	s.Cfg.VersioningEnabled = true
	s.Cfg.TrashRetention = time.Second
	if err = st.CreatePolka("docs"); err != nil {
		t.Fatal(err)
	}
	v, err := st.PutObject("docs", "x", bytes.NewBufferString("preserve"), "")
	if err != nil {
		t.Fatal(err)
	}
	s.Cfg.AccessPolicyFile = filepath.Join(t.TempDir(), "policy.json")
	policy := auth.Policy{Version: 1, Users: []auth.Principal{{ID: "writer", Role: "writer", Polki: []string{"docs"}, Keys: []auth.Key{{AccessKey: "writer", SecretKey: "writer-secret-key"}}}}}
	data, _ := json.Marshal(policy)
	if err = os.WriteFile(s.Cfg.AccessPolicyFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/docs/x?versionId=" + v.VersionID, 200},
		{"DELETE", "/docs/x?versionId=" + v.VersionID, 403},
		{"DELETE", "/docs/x", 204},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		signWith(r, nil, time.Now(), "writer", "writer-secret-key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
	s.Audit, err = audit.Open(filepath.Join(t.TempDir(), "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Audit.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.CleanupTrash(context.Background(), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("cleanup must fail closed when audit is unavailable")
	}
	versions, err := st.ListVersions("docs", "")
	if err != nil || len(versions) != 2 {
		t.Fatalf("history changed without audit: %v %v", versions, err)
	}
	r, _, err := st.GetVersion("docs", "x", v.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
}
