// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPolicyRejectsAmbiguousOrUnsafeConfiguration(t *testing.T) {
	valid := `{"version":1,"users":[{"id":"reader","role":"reader","polki":["demo"],"keys":[{"access_key":"read","secret_key":"example-secret-key"}]}]}`
	for _, body := range []string{
		strings.Replace(valid, `"version":1`, `"version":2`, 1),
		strings.Replace(valid, `"keys":[`, `"keys":[{"access_key":"read","secret_key":"another-secret-key"},`, 1),
		strings.Replace(valid, `"users":[`, `"users":[{"id":"reader","role":"admin"},`, 1),
		strings.Replace(valid, `"role":"reader"`, `"role":"owner"`, 1),
		strings.Replace(valid, `"polki":["demo"]`, `"polki":["demo","demo"]`, 1),
		strings.Replace(valid, `"polki":["demo"]`, `"polki":["../private"]`, 1),
		strings.Replace(valid, `"example-secret-key"`, `"short"`, 1),
		strings.Replace(valid, `"access_key":"read"`, `"access_key":"read/key"`, 1),
		strings.Replace(valid, `"version":1`, `"extra":"SECRET-NOT-FOR-LOGS","version":1`, 1),
		valid + "{}",
	} {
		path := filepath.Join(t.TempDir(), "policy.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPolicy(path); err == nil || strings.Contains(err.Error(), "SECRET-NOT-FOR-LOGS") {
			t.Fatalf("unsafe policy validation: %v", err)
		}
	}
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := LoadPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Credentials()) != 1 {
		t.Fatal("missing key")
	}
	u, err := p.Principal("read")
	if err != nil || !u.CanRead("demo") || u.CanWrite("demo") || u.CanRead("other") {
		t.Fatal("invalid grants")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPolicy(path); err == nil {
		t.Fatal("world-readable secrets accepted")
	}
}
