// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package storage

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rkislov/kladovka/internal/testprofile"
)

func BenchmarkProfileOverwriteLIST(b *testing.B) {
	s, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if err = s.CreatePolka("profile"); err != nil {
		b.Fatal(err)
	}
	body := bytes.Repeat([]byte{42}, 4096)
	sha, md := sha256.Sum256(body), md5.Sum(body)
	// Fast fixture setup, outside the timed/profiled section. No running server,
	// mutation or recovery behavior is inferred from these unjournaled seed files.
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("object-%06d", i)
		path := filepath.Join(s.root, "profile", key)
		if err = os.WriteFile(path, body, 0600); err != nil {
			b.Fatal(err)
		}
		meta := ObjectMeta{Key: key, Size: 4096, SHA256: hex.EncodeToString(sha[:]), ETag: fmt.Sprintf("\"%x\"", md), ContentType: "application/octet-stream"}
		raw, _ := json.Marshal(meta)
		if err = os.WriteFile(path+".kladovka-meta.json", raw, 0600); err != nil {
			b.Fatal(err)
		}
	}
	clear(s.catalogues) // fixture files were seeded without Store mutations
	if _, err = s.ListObjectPage("profile", "", "", "", 100); err != nil {
		b.Fatal(err)
	}
	b.Logf("exact filename semantics: %v", s.catalogueExactNames)
	stop := testprofile.Start(b, "storage")
	defer stop()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < 50*b.N; i++ {
		body[0] = byte(i)
		key := fmt.Sprintf("object-%06d", i%100)
		meta, err := s.PutObject("profile", key, bytes.NewReader(body), "application/octet-stream")
		if err != nil {
			b.Fatal(err)
		}
		page, err := s.ListObjectPage("profile", "", "", "", 100)
		if err != nil || len(page.Objects) != 100 || page.Objects[i%100].ETag != meta.ETag {
			b.Fatalf("invalid page: %v", err)
		}
	}
	b.StopTimer()
}
