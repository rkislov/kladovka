// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObjectRoutesDecodeKeysExactlyOnce(t *testing.T) {
	for _, prefix := range []string{"/", "/api/object/", "/internal/object/"} {
		t.Run(prefix, func(t *testing.T) {
			s, h := testServer(t, 1024)
			if err := s.Store.CreatePolka("demo"); err != nil {
				t.Fatal(err)
			}
			keys := []string{"nested/100%.txt", "nested/a%2Fb", "nested/a/b", "nested/%25", "nested/отчёт +.txt"}
			for i, key := range keys {
				data := []byte(fmt.Sprintf("object-%d", i))
				r := httptest.NewRequest("PUT", prefix+"demo/"+strings.ReplaceAll(escapeKeyPath(key), "+", "%2B"), bytes.NewReader(data))
				switch prefix {
				case "/":
					sign(r, data, time.Now())
				case "/api/object/":
					r.SetBasicAuth("test", "secret")
				default:
					r.Header.Set("Authorization", "Bearer cluster-secret")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 200 {
					t.Fatalf("PUT %q: %d %s", key, w.Code, w.Body.String())
				}
			}
			for i, key := range keys {
				// Read through a different route, so matching decode bugs cannot hide an alias.
				r := httptest.NewRequest("GET", "/demo/"+strings.ReplaceAll(escapeKeyPath(key), "+", "%2B"), nil)
				sign(r, nil, time.Now())
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 200 || w.Body.String() != fmt.Sprintf("object-%d", i) {
					t.Fatalf("GET %q: %d %s", key, w.Code, w.Body.String())
				}
			}
			objects, _, err := s.Store.ListObjects("demo", "", 100, "")
			if err != nil || len(objects) != len(keys) {
				t.Fatalf("catalogue: %v, %v", objects, err)
			}
			found := map[string]bool{}
			for _, o := range objects {
				found[o.Key] = true
			}
			for _, key := range keys {
				if !found[key] {
					t.Fatalf("stored key differs: %q", key)
				}
			}
		})
	}
}
