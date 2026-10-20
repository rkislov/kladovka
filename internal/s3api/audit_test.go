// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").
package s3api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/auth"
)

func TestAuditRecordsVerifiedActorWithoutCredentials(t *testing.T) {
	s, _ := testServer(t, 100)
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	var err error
	s.Audit, err = audit.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Audit.Close()
	h := s.Handler()
	if err = s.Store.CreatePolka("demo"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Store.PutObject("demo", "file", strings.NewReader("data"), ""); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("DELETE", "/demo/file?private=QUERY-SECRET", nil)
	sign(r, nil, time.Now())
	r.Header.Set("Cookie", "unrelated=COOKIE-SECRET")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	_, _ = login(t, h)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"QUERY-SECRET", "COOKIE-SECRET", "password", "Authorization", "X-Amz", "csrf", "secret"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("audit leaked %q", secret)
		}
	}
	var attempt, result audit.Event
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 4 {
		t.Fatalf("events: %s", data)
	}
	if json.Unmarshal(lines[0], &attempt) != nil || json.Unmarshal(lines[1], &result) != nil {
		t.Fatal("invalid JSONL")
	}
	if result.Actor != "test" || result.Method != "DELETE" || result.Path != "/demo/file" || result.Status != 204 || result.ID != attempt.ID || result.Time.IsZero() {
		t.Fatalf("missing attribution: %+v", result)
	}
	// A verified actor is retained when upload limiting clones the request context.
	r = httptest.NewRequest("PUT", "/demo/new", strings.NewReader("payload"))
	sign(r, []byte("payload"), time.Now())
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	data, _ = os.ReadFile(path)
	lines = bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	json.Unmarshal(lines[len(lines)-1], &result)
	if w.Code != 200 || result.Actor != "test" {
		t.Fatalf("lost upload actor: %d %+v", w.Code, result)
	}
	if err = s.Audit.Close(); err != nil {
		t.Fatal(err)
	}
	r = httptest.NewRequest("DELETE", "/demo/new", nil)
	sign(r, nil, time.Now())
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatal("audit failure did not block mutation")
	}
	rc, _, err := s.Store.GetObject("demo", "new")
	if err != nil {
		t.Fatal("mutation ran without audit")
	}
	rc.Close()
}
func TestPolicyAuditCapturesKeyAndRoleChanges(t *testing.T) {
	s, _ := testServer(t, 100)
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	var err error
	s.Audit, err = audit.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Audit.Close()
	p := &auth.Policy{Version: 1, Users: []auth.Principal{{ID: "alice", Role: "writer", Polki: []string{"demo"}, Keys: []auth.Key{{AccessKey: "key-one", SecretKey: "SECRET-NEVER-LOG-THIS"}}}}}
	for i := 0; i < 2; i++ {
		if err = s.RecordPolicy(p); err != nil {
			t.Fatal(err)
		}
	}
	p.Users[0].Keys[0].SecretKey = "ROTATED-SECRET-NEVER-LOG"
	if err = s.RecordPolicy(p); err != nil {
		t.Fatal(err)
	}
	p.Users[0].Role = "reader"
	p.Users[0].Keys = nil
	if err = s.RecordPolicy(p); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if bytes.Contains(data, []byte("SECRET")) || bytes.Contains(data, []byte("secret_key")) {
		t.Fatal("policy secrets leaked")
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("policy changes: %s", data)
	}
	var last audit.Event
	json.Unmarshal(lines[2], &last)
	if last.Actor != "local_configuration" || last.Users[0].Role != "reader" || len(last.Users[0].Keys) != 0 {
		t.Fatalf("policy audit: %+v", last)
	}
}
