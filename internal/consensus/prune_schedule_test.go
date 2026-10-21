// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

type pruneCheckReader struct {
	io.Reader
	check func()
}

func (r *pruneCheckReader) Read(p []byte) (int, error) {
	if r.check != nil {
		r.check()
		r.check = nil
	}
	return r.Reader.Read(p)
}

func TestDuePruneRunsBeforeUploadRead(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	if err := s.CreatePolka("prune"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutObject("prune", "key", strings.NewReader("old"), ""); err != nil {
		t.Fatal(err)
	}
	old, _ := s.record("prune", "key")
	if _, err := s.PutObject("prune", "key", strings.NewReader("current"), ""); err != nil {
		t.Fatal(err)
	}
	current, _ := s.record("prune", "key")
	s.writeMu.Lock()
	s.lastPruneAttempt = time.Now().Add(-2 * time.Minute)
	s.writeMu.Unlock()
	body := bytes.Repeat([]byte("next generation"), ChunkSize/4)
	reader := &pruneCheckReader{Reader: bytes.NewReader(body), check: func() {
		// The upload owns writeMu here; the background TryLock cannot run now.
		s.fsm.mu.RLock()
		_, retained := s.fsm.state.Chunks[old.Chunks[0].Hash]
		s.fsm.mu.RUnlock()
		if retained {
			t.Error("expired maintenance was starved by foreground upload")
		}
		if _, err := os.Stat(s.fsm.path(old.Chunks[0].Hash)); !errors.Is(err, os.ErrNotExist) {
			t.Error("unreferenced chunk remains")
		}
		if _, err := s.fsm.chunk(current.Chunks[0].Hash); err != nil {
			t.Error("current generation pruned")
		}
	}}
	if _, err := s.PutObject("prune", "key", reader, ""); err != nil {
		t.Fatal(err)
	}
	r, _, err := s.GetObject("prune", "key")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, body) {
		t.Fatal("new generation damaged")
	}
	s.writeMu.Lock()
	last := s.lastPruneAttempt
	s.pruneIfDue()
	if !s.lastPruneAttempt.Equal(last) {
		t.Error("maintenance retried before interval")
	}
	s.writeMu.Unlock()
}
