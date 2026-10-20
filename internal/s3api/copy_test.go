// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/auth"
)

func copyRequest(h http.Handler, source, target string, headers map[string]string, body []byte, access, secret string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("PUT", target, bytes.NewReader(body))
	r.Header.Set("X-Amz-Copy-Source", source)
	signed := []string{"x-amz-copy-source"}
	for name, value := range headers {
		r.Header.Set(name, value)
		signed = append(signed, strings.ToLower(name))
	}
	signWith(r, body, time.Now(), access, secret, signed...)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestCopyObjectContracts(t *testing.T) {
	s, h := testServer(t, 1024)
	for _, bucket := range []string{"source", "target"} {
		if e := s.Store.CreatePolka(bucket); e != nil {
			t.Fatal(e)
		}
	}
	meta, e := s.Store.PutObject("source", "file%2F+.txt", strings.NewReader("original"), "text/plain")
	if e != nil {
		t.Fatal(e)
	}
	source := "/source/file%252F%2B.txt"
	copy := func(target string, headers map[string]string) *httptest.ResponseRecorder {
		return copyRequest(h, source, target, headers, nil, "test", "secret")
	}
	w := copy("/target/copy", nil)
	var result struct {
		ETag         string
		LastModified string
	}
	if w.Code != 200 || xml.Unmarshal(w.Body.Bytes(), &result) != nil || result.ETag != meta.ETag || result.LastModified == "" {
		t.Fatalf("copy: %d %s", w.Code, w.Body.String())
	}
	r, m, e := s.Store.GetObject("target", "copy")
	if e != nil {
		t.Fatal(e)
	}
	data, _ := io.ReadAll(r)
	r.Close()
	if string(data) != "original" || m.ContentType != "text/plain" {
		t.Fatal(string(data), m)
	}
	cases := []struct {
		name    string
		headers map[string]string
		status  int
	}{
		{"match", map[string]string{"X-Amz-Copy-Source-If-Match": meta.ETag}, 200},
		{"match-unquoted", map[string]string{"X-Amz-Copy-Source-If-Match": strings.Trim(meta.ETag, "\"")}, 200},
		{"match-fails", map[string]string{"X-Amz-Copy-Source-If-Match": "\"wrong\""}, 412},
		{"none-fails", map[string]string{"X-Amz-Copy-Source-If-None-Match": meta.ETag}, 412},
		{"old-unmodified", map[string]string{"X-Amz-Copy-Source-If-Unmodified-Since": meta.LastModified.Add(-time.Hour).Format(http.TimeFormat)}, 412},
		{"new-modified", map[string]string{"X-Amz-Copy-Source-If-Modified-Since": meta.LastModified.Add(time.Hour).Format(http.TimeFormat)}, 412},
		{"match-precedence", map[string]string{"X-Amz-Copy-Source-If-Match": meta.ETag, "X-Amz-Copy-Source-If-Unmodified-Since": meta.LastModified.Add(-time.Hour).Format(http.TimeFormat)}, 200},
		{"invalid-date", map[string]string{"X-Amz-Copy-Source-If-Modified-Since": "bad"}, 400},
		{"metadata", map[string]string{"X-Amz-Meta-Owner": "ignored?"}, 501},
		{"encryption", map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}, 501},
		{"destination-condition", map[string]string{"If-Match": meta.ETag}, 501},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := copy("/target/copy", tc.headers)
			if w.Code != tc.status {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			got, e := s.Store.HeadObject("target", "copy")
			if e != nil || got.ETag != meta.ETag {
				t.Fatal("destination changed", got, e)
			}
		})
	}
	if w := copy("/missing/copy", nil); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := copyRequest(h, "/source/missing", "/target/copy", nil, nil, "test", "secret"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := copyRequest(h, source, "/target/copy", nil, []byte("not empty"), "test", "secret"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := copyRequest(h, "/source/file%252F%2B.txt?versionId=x", "/target/copy", nil, nil, "test", "secret"); w.Code != 501 {
		t.Fatal(w.Code)
	}
	if w := copy("/target/copy?uploadId=abc&partNumber=1", nil); w.Code != 501 {
		t.Fatal(w.Code)
	}
	if w := copy("/source/file%252F%2B.txt", nil); w.Code != 400 {
		t.Fatal("self-copy", w.Code)
	}
	if w := copy("/source/file%252F%2B.txt", map[string]string{"X-Amz-Metadata-Directive": "REPLACE", "Content-Type": "application/json"}); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	m, e = s.Store.HeadObject("source", "file%2F+.txt")
	if e != nil || m.ContentType != "application/json" {
		t.Fatal(m, e)
	}
	// Limit failures must preserve the existing destination and clean up the spool.
	s.Cfg.MaxObjectSize = 2
	if w := copy("/target/copy", nil); w.Code != 413 {
		t.Fatal("size", w.Code, w.Body.String())
	}
	s.Cfg.MaxObjectSize = 1024
	s.Cfg.MaxTempBytes = 2
	if w := copy("/target/copy", nil); w.Code < 400 {
		t.Fatal("temporary quota", w.Code)
	}
	s.Cfg.MaxTempBytes = 0
	if e := os.WriteFile(filepath.Join(filepath.Dir(s.Store.UploadDir()), "source", "file%2F+.txt"), []byte("tampered"), 0600); e != nil {
		t.Fatal(e)
	}
	if w := copy("/target/copy", nil); w.Code != 500 {
		t.Fatal("corrupt copy source accepted", w.Code, w.Body.String())
	}
	reader, _, e := s.Store.GetObject("target", "copy")
	if e != nil {
		t.Fatal(e)
	}
	unchanged, _ := io.ReadAll(reader)
	reader.Close()
	if string(unchanged) != "original" {
		t.Fatal("failed copy changed destination")
	}
	entries, _ := os.ReadDir(s.Store.UploadDir())
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "upload-") {
			t.Fatal("leaked spool", entry.Name())
		}
	}
}
func TestCopySourceAuthorizationAndSignedHeaders(t *testing.T) {
	s, h := testServer(t, 1024)
	s.Cfg.AccessPolicyFile = filepath.Join(t.TempDir(), "policy.json")
	p := auth.Policy{Version: 1, Users: []auth.Principal{
		{ID: "limited", Role: "writer", Polki: []string{"target"}, Keys: []auth.Key{{AccessKey: "limited", SecretKey: "limited-secret-key"}}},
		{ID: "reader", Role: "reader", Polki: []string{"source", "target"}, Keys: []auth.Key{{AccessKey: "reader", SecretKey: "reader-secret-key"}}},
		{ID: "both", Role: "writer", Polki: []string{"source", "target"}, Keys: []auth.Key{{AccessKey: "both", SecretKey: "both-secret-key-xx"}}},
	}}
	b, _ := json.Marshal(p)
	if e := os.WriteFile(s.Cfg.AccessPolicyFile, b, 0600); e != nil {
		t.Fatal(e)
	}
	for _, bucket := range []string{"source", "target"} {
		s.Store.CreatePolka(bucket)
	}
	s.Store.PutObject("source", "secret", strings.NewReader("private"), "text/plain")
	for _, tc := range []struct {
		access, secret string
		status         int
	}{{"limited", "limited-secret-key", 403}, {"reader", "reader-secret-key", 403}, {"both", "both-secret-key-xx", 200}} {
		w := copyRequest(h, "/source/secret", "/target/copy", nil, nil, tc.access, tc.secret)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.access, w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest("PUT", "/target/unsigned", nil)
	signWith(r, nil, time.Now(), "both", "both-secret-key-xx")
	r.Header.Set("X-Amz-Copy-Source", "/source/secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("unsigned source accepted", w.Code)
	}
	if _, e := s.Store.HeadObject("target", "unsigned"); e == nil {
		t.Fatal("unsigned copy published")
	}
}
