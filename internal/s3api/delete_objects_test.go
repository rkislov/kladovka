// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"hash/crc64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/auth"
)

func batchRequest(h http.Handler, path, body, access, secret string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	sum := md5.Sum([]byte(body))
	r.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
	for name, value := range headers {
		if value == "" {
			r.Header.Del(name)
		} else {
			r.Header.Set(name, value)
		}
	}
	extra := []string{}
	for name := range r.Header {
		extra = append(extra, strings.ToLower(name))
	}
	signWith(r, []byte(body), time.Now(), access, secret, extra...)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func decodeBatch(t *testing.T, w *httptest.ResponseRecorder) deleteResult {
	t.Helper()
	var result deleteResult
	if w.Code != 200 || xml.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatalf("batch: %d %s", w.Code, w.Body.String())
	}
	return result
}

type deleteFaultStore struct {
	Backend
	fail func(string, string) error
}

func (s deleteFaultStore) DeleteObject(bucket, key string) error {
	if e := s.fail(bucket, key); e != nil {
		return e
	}
	return s.Backend.DeleteObject(bucket, key)
}

func TestDeleteObjectsResultsQuietAndLiteralKeys(t *testing.T) {
	s, h := testServer(t, 4096)
	s.Store.CreatePolka("demo")
	for _, key := range []string{"ok", "fail", "a%2Fb", "a/b"} {
		s.Store.PutObject("demo", key, strings.NewReader(key), "text/plain")
	}
	calls := map[string]int{}
	s.Store = deleteFaultStore{s.Store, func(bucket, key string) error {
		calls[key]++
		if key == "fail" {
			return errors.New("injected delete failure")
		}
		return nil
	}}
	body := `<Delete><Object><Key>ok</Key></Object><Object><Key>fail</Key></Object><Object><Key>missing</Key></Object><Object><Key>a%2Fb</Key></Object><Object><Key>ok</Key></Object><Object><Key>../escape</Key></Object></Delete>`
	result := decodeBatch(t, batchRequest(h, "/demo?delete", body, "test", "secret", nil))
	if len(result.Deleted) != 4 || len(result.Errors) != 2 || result.Errors[0].Key != "fail" || result.Errors[1].Code != "InvalidArgument" || calls["ok"] != 1 {
		t.Fatalf("results %+v calls %v", result, calls)
	}
	if _, e := s.Store.HeadObject("demo", "a/b"); e != nil {
		t.Fatal("literal percent key aliased", e)
	}
	if _, e := s.Store.HeadObject("demo", "a%2Fb"); e == nil {
		t.Fatal("literal key not deleted")
	}
	quiet := `<Delete><Object><Key>fail</Key></Object><Object><Key>a/b</Key></Object><Quiet>true</Quiet></Delete>`
	result = decodeBatch(t, batchRequest(h, "/demo?delete", quiet, "test", "secret", nil))
	if len(result.Deleted) != 0 || len(result.Errors) != 1 || result.Errors[0].Key != "fail" {
		t.Fatalf("quiet %+v", result)
	}
	result = decodeBatch(t, batchRequest(h, "/demo?delete", `<Delete><Object><Key>missing</Key></Object><Quiet>true</Quiet></Delete>`, "test", "secret", nil))
	if len(result.Deleted) != 0 || len(result.Errors) != 0 {
		t.Fatal(result)
	}
}
func TestDeleteObjectsRejectBeforeAnyMutation(t *testing.T) {
	s, h := testServer(t, 4096)
	s.Store.CreatePolka("demo")
	s.Store.PutObject("demo", "kept", strings.NewReader("keep"), "")
	valid := `<Delete><Object><Key>kept</Key></Object></Delete>`
	cases := []struct {
		name, body string
		headers    map[string]string
		status     int
	}{
		{"missing-checksum", valid, map[string]string{"Content-MD5": ""}, 400},
		{"wrong-checksum", valid, map[string]string{"Content-MD5": "wrong"}, 400},
		{"wrong-crc64", valid, map[string]string{"X-Amz-Checksum-Crc64nvme": "wrong"}, 400},
		{"truncated", valid[:len(valid)-1], nil, 400},
		{"trailing", valid + valid, nil, 400},
		{"duplicate-key", `<Delete><Object><Key>kept</Key><Key>other</Key></Object></Delete>`, nil, 400},
		{"late-version", `<Delete><Object><Key>kept</Key></Object><Object><Key>other</Key><VersionId>null</VersionId></Object></Delete>`, nil, 501},
		{"late-condition", `<Delete><Object><Key>kept</Key></Object><Object><Key>other</Key><ETag>value</ETag></Object></Delete>`, nil, 501},
		{"unknown", `<Delete><Object><Key>kept</Key></Object><Other/></Delete>`, nil, 400},
		{"invalid-quiet", `<Delete><Object><Key>kept</Key></Object><Quiet>yes</Quiet></Delete>`, nil, 400},
		{"empty", `<Delete/>`, nil, 400},
		{"too-many", `<Delete>` + strings.Repeat(`<Object><Key>kept</Key></Object>`, 1001) + `</Delete>`, nil, 400},
		{"too-large", valid + strings.Repeat(" ", maxDeleteBody), nil, 400},
		{"mfa", valid, map[string]string{"X-Amz-Mfa": "unsupported"}, 501},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := batchRequest(h, "/demo?delete", tc.body, "test", "secret", tc.headers)
			if w.Code != tc.status {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if _, e := s.Store.HeadObject("demo", "kept"); e != nil {
				t.Fatal("mutated before validation", e)
			}
		})
	}
	// CRC/SHA validation supplements, never replaces, the signed payload check.
	r := httptest.NewRequest("POST", "/demo?delete", strings.NewReader(valid))
	sum := md5.Sum([]byte(valid))
	r.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
	signWith(r, []byte("different"), time.Now(), "test", "secret", "content-md5")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	if _, e := s.Store.HeadObject("demo", "kept"); e != nil {
		t.Fatal(e)
	}
	sha := sha256.Sum256([]byte(valid))
	result := decodeBatch(t, batchRequest(h, "/demo?delete", valid, "test", "secret", map[string]string{"Content-MD5": "", "X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(sha[:]), "X-Amz-Sdk-Checksum-Algorithm": "SHA256"}))
	if len(result.Deleted) != 1 {
		t.Fatal(result)
	}
}
func TestDeleteObjectsPolicyAndAudit(t *testing.T) {
	s, _ := testServer(t, 1024)
	s.Store.CreatePolka("demo")
	s.Store.PutObject("demo", "kept", strings.NewReader("keep"), "")
	s.Cfg.AccessPolicyFile = filepath.Join(t.TempDir(), "policy.json")
	policy := auth.Policy{Version: 1, Users: []auth.Principal{
		{ID: "reader", Role: "reader", Polki: []string{"demo"}, Keys: []auth.Key{{AccessKey: "read", SecretKey: "reader-secret-key"}}},
		{ID: "foreign", Role: "writer", Polki: []string{"other"}, Keys: []auth.Key{{AccessKey: "foreign", SecretKey: "foreign-secret-key"}}},
		{ID: "writer", Role: "writer", Polki: []string{"demo"}, Keys: []auth.Key{{AccessKey: "write", SecretKey: "writer-secret-key"}}},
	}}
	raw, _ := json.Marshal(policy)
	os.WriteFile(s.Cfg.AccessPolicyFile, raw, 0600)
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	var err error
	s.Audit, err = audit.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Audit.Close()
	h := s.Handler()
	body := `<Delete><Object><Key>kept</Key></Object></Delete>`
	for _, tc := range []struct{ access, secret string }{{"read", "reader-secret-key"}, {"foreign", "foreign-secret-key"}} {
		w := batchRequest(h, "/demo?delete", body, tc.access, tc.secret, nil)
		if w.Code != 403 {
			t.Fatal(tc.access, w.Code, w.Body.String())
		}
		if _, e := s.Store.HeadObject("demo", "kept"); e != nil {
			t.Fatal(e)
		}
	}
	decodeBatch(t, batchRequest(h, "/demo?delete", body, "write", "writer-secret-key", nil))
	data, _ := os.ReadFile(path)
	found := false
	for _, line := range bytes.Split(data, []byte("\n")) {
		var event audit.Event
		if json.Unmarshal(line, &event) == nil && event.Kind == "result" && event.Method == "DELETE" && event.Path == "/demo/kept" && event.Actor == "writer" && event.Status == 204 {
			found = true
		}
	}
	if !found {
		t.Fatal("missing per-key audit", string(data))
	}
	if bytes.Contains(data, []byte("writer-secret-key")) {
		t.Fatal("secret in audit")
	}
}

// Vectors from awslabs/aws-checksums/tests/crc64_test.c.
func TestDeleteCRC64NVME(t *testing.T) {
	for _, tt := range []struct {
		data []byte
		want uint64
	}{
		{[]byte("123456789"), 0xae8b14860a799888},
		{make([]byte, 32), 0xcf3473434d4ecf3b},
	} {
		if got := crc64.Checksum(tt.data, crc64NVMETable); got != tt.want {
			t.Fatalf("got %x want %x", got, tt.want)
		}
	}
}
