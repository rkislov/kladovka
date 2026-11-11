// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"
)

// More than eight chunk commands forces eviction before snapshot/restart.
// Neither a warm log cache nor an un-compacted WAL may be needed to read data.
func TestRecentLogsSnapshotAndRestart(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	if err := s.CreatePolka("cached"); err != nil {
		t.Fatal(err)
	}
	bodies := make([][]byte, 12)
	for i := range bodies {
		bodies[i] = make([]byte, 1<<20)
		rand.New(rand.NewSource(int64(901 + i))).Read(bodies[i])
		if _, err := s.PutObject("cached", fmt.Sprint(i), bytes.NewReader(bodies[i]), ""); err != nil {
			t.Fatal(err)
		}
	}
	boundary := s.fsm.copyState().Index
	eventually(t, 10*time.Second, func() bool {
		for _, node := range f.nodes {
			if node.fsm.copyState().Index < boundary {
				return false
			}
		}
		return true
	})
	for _, node := range f.nodes {
		if err := node.raft.Snapshot().Error(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range f.nodes {
		f.restart(t, i)
	}
	s = f.nodes[f.leader(t)]
	for i, body := range bodies {
		reader, _, err := s.GetObject("cached", fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("object %d after snapshot/restart: %v", i, err)
		}
	}
}
