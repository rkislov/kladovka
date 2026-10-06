// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");

package storage

import (
	"bytes"
	"io"
	"path/filepath"
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
