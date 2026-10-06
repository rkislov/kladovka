// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");

package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPolkaPutGet(t *testing.T) {
	dir := t.TempDir()
	st, err := New(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreatePolka("архив"); err != nil {
		t.Fatal(err)
	}
	meta, err := st.PutObject("архив", "a/b.txt", bytes.NewReader([]byte("привет")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 12 && meta.Size != int64(len([]byte("привет"))) {
		// UTF-8 length
		if meta.Size != int64(len([]byte("привет"))) {
			t.Fatalf("size=%d", meta.Size)
		}
	}
	rc, got, err := st.GetObject("архив", "a/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	buf := make([]byte, got.Size)
	n, _ := rc.Read(buf)
	if string(buf[:n]) != "привет" {
		t.Fatalf("body=%q", buf[:n])
	}
	_ = os.RemoveAll(dir)
}
