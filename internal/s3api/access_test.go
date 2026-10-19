// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/auth"
)

func TestPolicyAccessMatrixAndRevocation(t *testing.T) {
	s, h := testServer(t, 1024)
	s.Cfg.AdminUser, s.Cfg.AdminPassword = "operator", "operator-password-only"
	s.Cfg.AccessPolicyFile = filepath.Join(t.TempDir(), "access.json")
	p := auth.Policy{Version: 1, Users: []auth.Principal{
		{ID: "reader", Role: "reader", Polki: []string{"shared"}, Keys: []auth.Key{{AccessKey: "read", SecretKey: "reader-secret-key"}, {AccessKey: "read2", SecretKey: "reader-second-key"}}},
		{ID: "writer", Role: "writer", Polki: []string{"shared"}, Keys: []auth.Key{{AccessKey: "write", SecretKey: "writer-secret-key"}}},
		{ID: "service-admin", Role: "admin", Keys: []auth.Key{{AccessKey: "manage", SecretKey: "manage-secret-key"}}},
	}}
	save := func() {
		t.Helper()
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		// Same-directory rename is the documented live-update protocol.
		if err := os.WriteFile(s.Cfg.AccessPolicyFile+".new", b, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(s.Cfg.AccessPolicyFile+".new", s.Cfg.AccessPolicyFile); err != nil {
			t.Fatal(err)
		}
	}
	save()
	for _, name := range []string{"shared", "private"} {
		if err := s.Store.CreatePolka(name); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Store.PutObject(name, "file", strings.NewReader("hello"), "text/plain"); err != nil {
			t.Fatal(err)
		}
	}
	keys := map[string]string{"read": "reader-secret-key", "read2": "reader-second-key", "write": "writer-secret-key", "manage": "manage-secret-key", "test": "secret"}
	do := func(key, method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		signWith(r, []byte(body), time.Now(), key, keys[key])
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for _, tc := range []struct {
		key, method, path string
		status            int
	}{
		{"read", "GET", "/shared/file", 200}, {"read", "HEAD", "/shared/file", 200},
		{"read", "GET", "/private/file", 403}, {"read", "HEAD", "/private/file", 403},
		{"read", "PUT", "/shared/new", 403}, {"read", "DELETE", "/shared/file", 403},
		{"read", "POST", "/shared/file?uploads", 403}, {"read", "GET", "/shared/file?uploadId=bad", 403},
		{"read", "GET", "/api/polki/shared", 200}, {"read", "GET", "/api/polki/private", 403},
		{"read", "GET", "/api/object/shared/file", 200}, {"read", "GET", "/api/object/private/file", 403},
		{"read", "GET", "/api/stats", 403}, {"read", "POST", "/api/polki", 403},
		{"write", "PUT", "/shared/new", 200}, {"write", "DELETE", "/shared/new", 204},
		{"write", "PUT", "/private/new", 403}, {"write", "DELETE", "/private/file", 403},
		{"write", "PUT", "/new-shelf", 403}, {"write", "DELETE", "/shared", 403},
		{"write", "PUT", "/api/object/shared/new", 200}, {"write", "DELETE", "/api/object/shared/new", 204},
		{"write", "PUT", "/api/object/private/new", 403}, {"write", "DELETE", "/api/polki/shared", 403},
		{"manage", "GET", "/private/file", 200}, {"manage", "GET", "/api/stats", 200},
		{"test", "GET", "/shared/file", 403},
	} {
		t.Run(tc.key+tc.method+tc.path, func(t *testing.T) {
			if w := do(tc.key, tc.method, tc.path, ""); w.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.status, w.Body.String())
			}
		})
	}
	for _, path := range []string{"/", "/api/polki", "/api/polki/"} {
		w := do("read", "GET", path, "")
		if w.Code != 200 || !strings.Contains(w.Body.String(), "shared") || strings.Contains(w.Body.String(), "private") {
			t.Fatalf("unfiltered listing %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	// Writers can complete multipart only within their assigned shelf.
	w := do("write", "POST", "/shared/multi?uploads", "")
	var initiated initiateMultipartUploadResult
	if w.Code != 200 || xml.Unmarshal(w.Body.Bytes(), &initiated) != nil || initiated.UploadID == "" {
		t.Fatalf("initiate multipart: %s", w.Body.String())
	}
	path := "/shared/multi?uploadId=" + initiated.UploadID
	w = do("write", "PUT", path+"&partNumber=1", "multipart body")
	if w.Code != 200 {
		t.Fatalf("upload part: %s", w.Body.String())
	}
	completion := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + w.Header().Get("ETag") + "</ETag></Part></CompleteMultipartUpload>"
	if w := do("read", "GET", path, ""); w.Code != 403 {
		t.Fatal("reader listed incomplete parts")
	}
	if w := do("write", "POST", "/private/multi?uploadId="+initiated.UploadID, completion); w.Code != 403 {
		t.Fatal("multipart crossed shelf boundary")
	}
	if w := do("write", "POST", path, completion); w.Code != 200 {
		t.Fatalf("complete: %s", w.Body.String())
	}
	if w := do("read", "GET", "/shared/multi", ""); w.Code != 200 || w.Body.String() != "multipart body" {
		t.Fatal("completed multipart not readable")
	}
	// JSON admin writes must validate the signed payload as well as the signature.
	r := httptest.NewRequest("POST", "/api/polki", strings.NewReader(`{"name":"tampered"}`))
	signWith(r, []byte(`{"name":"original"}`), time.Now(), "manage", keys["manage"])
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("tampered JSON signature accepted")
	}
	// Basic credentials are solely for the operator; neither legacy nor S3 keys work.
	for _, tc := range []struct {
		user, pass string
		status     int
	}{{"operator", "operator-password-only", 200}, {"test", "secret", 401}, {"read", "reader-secret-key", 401}} {
		r := httptest.NewRequest("GET", "/api/stats", nil)
		r.SetBasicAuth(tc.user, tc.pass)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("basic %s: %d", tc.user, w.Code)
		}
	}
	// Deleting one key revokes that key without deleting its sibling or restarting.
	p.Users[1].Role = "reader"
	save()
	if w := do("write", "PUT", "/shared/revoked-write", "body"); w.Code != 403 {
		t.Fatal("old writer role was cached")
	}
	if w := do("write", "GET", "/shared/file", ""); w.Code != 200 {
		t.Fatal("reader access lost after role change")
	}
	p.Users[0].Keys = p.Users[0].Keys[1:]
	save()
	if w := do("read", "GET", "/shared/file", ""); w.Code != 403 {
		t.Fatal("revoked key accepted")
	}
	if w := do("read2", "GET", "/shared/file", ""); w.Code != 200 {
		t.Fatal("sibling key lost")
	}
	// A malformed policy never restores ACCESS_KEY/SECRET_KEY or cached access.
	if err := os.WriteFile(s.Cfg.AccessPolicyFile, []byte(`{"version":`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"test", "read2", "manage"} {
		if w := do(key, "GET", "/shared/file", ""); w.Code != 403 {
			t.Fatalf("broken policy accepted %s", key)
		}
	}
	if err := os.Remove(s.Cfg.AccessPolicyFile); err != nil {
		t.Fatal(err)
	}
	if w := do("read2", "GET", "/shared/file", ""); w.Code != 403 {
		t.Fatal("missing policy accepted")
	}
	// Denied writes did not alter existing data.
	rc, _, err := s.Store.GetObject("private", "file")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	var b bytes.Buffer
	b.ReadFrom(rc)
	if b.String() != "hello" {
		t.Fatal("denied mutation changed data")
	}
}
