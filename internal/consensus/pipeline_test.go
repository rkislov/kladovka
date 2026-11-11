// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"

	"github.com/rkislov/kladovka/internal/storage"
)

type brokenUpload struct{ remaining, offset int }

func (r *brokenUpload) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	n := min(r.remaining, len(p))
	for i := range p[:n] {
		p[i] = byte(r.offset + i + (r.offset+i)/ChunkSize)
	}
	r.remaining -= n
	r.offset += n
	return n, nil
}

func TestPipelinedUploadDoesNotPublishAfterChunkOrReaderFailure(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	var armed atomic.Bool
	var calls atomic.Int64
	// Install before the first chunk exists; the closure itself remains immutable
	// while FSM and repair workers run. This injects a leader's durable save failure.
	originalFree := s.fsm.free
	s.fsm.free = func(n uint64) error {
		if armed.Load() && calls.Add(1) == 3 {
			return storage.ErrInsufficientSpace
		}
		return originalFree(n)
	}
	if err := s.CreatePolka("pipeline"); err != nil {
		t.Fatal(err)
	}
	old, err := s.PutObject("pipeline", "target", bytes.NewReader([]byte("old acknowledged bytes")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 5*ChunkSize+17)
	rand.New(rand.NewSource(42)).Read(body)
	armed.Store(true)
	if _, err = s.PutObject("pipeline", "target", bytes.NewReader(body), "application/octet-stream"); !errors.Is(err, storage.ErrInsufficientSpace) {
		t.Fatal("disk failure not propagated", err)
	}
	armed.Store(false)
	checkOld := func() {
		r, err := s.record("pipeline", "target")
		if err != nil || r.Meta.SHA256 != old.SHA256 {
			t.Fatal("failed upload published", r, err)
		}
		if !s.writeMu.TryLock() {
			t.Fatal("failed upload did not release lock")
		}
		s.writeMu.Unlock()
	}
	checkOld()
	if _, err = s.PutObject("pipeline", "target", &brokenUpload{remaining: 2 * ChunkSize}, ""); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("reader error lost", err)
	}
	checkOld()
	partial, _ := io.ReadAll(&brokenUpload{remaining: 2 * ChunkSize})
	for offset := 0; offset < len(partial); offset += ChunkSize {
		sum := sha256.Sum256(partial[offset : offset+ChunkSize])
		if _, err := s.fsm.chunk(hex.EncodeToString(sum[:])); err != nil {
			t.Fatal("reader failure returned before queued chunks settled", err)
		}
	}
	meta, err := s.PutObject("pipeline", "target", bytes.NewReader(body), "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	reader, got, err := s.GetObject("pipeline", "target")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.Equal(data, body) || got.SHA256 != meta.SHA256 {
		t.Fatal("retry lost chunk data", err)
	}
	// A committed upload remains readable through a different leader after restart.
	for i := range f.nodes {
		f.restart(t, i)
	}
	reader, got, err = f.nodes[f.leader(t)].GetObject("pipeline", "target")
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.Equal(data, body) || got.SHA256 != meta.SHA256 {
		t.Fatal("restart lost pipelined data", err)
	}
}

func TestVerifiedChunkReadBounds(t *testing.T) {
	m := &machine{dir: t.TempDir()}
	for _, size := range []int{0, 1, ChunkSize} {
		body := bytes.Repeat([]byte{byte(size % 251)}, size)
		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])
		if err := m.save(hash, body); err != nil {
			t.Fatal(err)
		}
		got, err := m.chunk(hash)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatal(size, err)
		}
		if err := os.WriteFile(m.path(hash), append(body, 1), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := m.chunk(hash); !errors.Is(err, storage.ErrIntegrity) {
			t.Fatal("oversized/corrupt bytes accepted", size, err)
		}
	}
}
