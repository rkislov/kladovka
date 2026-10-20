// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");

package storage

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestMultipartRoundTrip(t *testing.T) {
	st, err := New(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreatePolka("полка"); err != nil {
		t.Fatal(err)
	}
	id, err := st.CreateMultipartUpload("полка", "big.bin", "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	p1, err := st.UploadPart(id, 1, bytes.NewReader([]byte("hello ")))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := st.UploadPart(id, 2, bytes.NewReader([]byte("world")))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := st.CompleteMultipartUpload(id, []MultipartPart{*p1, *p2})
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 11 {
		t.Fatalf("size=%d", meta.Size)
	}
	rc, _, err := st.GetObject("полка", "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if string(b) != "hello world" {
		t.Fatalf("body=%q", b)
	}
}

// Clients may submit the opaque ETag with or without its HTTP quotes.
func TestMultipartETagRepresentations(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transform func(string) string
		valid     bool
	}{
		{"quoted", func(s string) string { return s }, true},
		{"unquoted", func(s string) string { return strings.Trim(s, `"`) }, true},
		{"wrong", func(s string) string { return `"00000000000000000000000000000000"` }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := st.CreatePolka("files"); err != nil {
				t.Fatal(err)
			}
			id, err := st.CreateMultipartUpload("files", "file", "text/plain")
			if err != nil {
				t.Fatal(err)
			}
			part, err := st.UploadPart(id, 1, strings.NewReader("hello"))
			if err != nil {
				t.Fatal(err)
			}
			part.ETag = tc.transform(part.ETag)
			_, err = st.CompleteMultipartUpload(id, []MultipartPart{*part})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}
