// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rkislov/kladovka/internal/storage"
)

func TestApplyBatchBoundsErrorsAndOrdering(t *testing.T) {
	m := &machine{dir: t.TempDir(), state: emptyState()}
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var active, peak atomic.Int32
	m.free = func(n uint64) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		if n == 17 {
			return storage.ErrInsufficientSpace
		}
		return nil
	}
	logs := []*raft.Log{}
	add := func(c command) {
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, &raft.Log{Index: uint64(len(logs) + 1), Type: raft.LogCommand, Data: data})
	}
	add(command{Kind: "create", Polka: "before"})
	logs = append(logs, &raft.Log{Index: 2, Type: raft.LogConfiguration, Data: []byte("not a command")})
	hashes := []string{}
	for i := 0; i < 6; i++ {
		n := 16
		if i == 2 {
			n = 17
		}
		body := bytes.Repeat([]byte{byte(i)}, n)
		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])
		hashes = append(hashes, hash)
		add(command{Kind: "chunk", Hash: hash, Data: body})
	}
	add(command{Kind: "chunk", Hash: hashes[0], Data: []byte("bad hash")})
	logs = append(logs, &raft.Log{Index: 10, Type: raft.LogCommand, Data: []byte("{")})
	add(command{Kind: "create", Polka: "after"})
	done := make(chan []interface{}, 1)
	go func() { done <- m.ApplyBatch(logs) }()
	// The first wave must overlap; a fifth save must wait for the bounded wave.
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("save wave did not overlap")
		}
	}
	select {
	case <-entered:
		close(release)
		t.Fatal("more than four concurrent saves")
	case <-done:
		close(release)
		t.Fatal("batch returned before saves")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	var results []interface{}
	select {
	case results = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not finish")
	}
	if len(results) != len(logs) || peak.Load() > 4 {
		t.Fatal("batch bounds", len(results), peak.Load())
	}
	for i, r := range results {
		switch i {
		case 4:
			if !errors.Is(r.(error), storage.ErrInsufficientSpace) {
				t.Fatal("wrong disk error", r)
			}
		case 8:
			if !errors.Is(r.(error), storage.ErrIntegrity) {
				t.Fatal("wrong integrity error", r)
			}
		case 9:
			if r == nil {
				t.Fatal("malformed command accepted")
			}
		default:
			if r != nil {
				t.Fatal("unexpected response", i, r)
			}
		}
	}
	for i, h := range hashes {
		_, err := m.chunk(h)
		if i == 2 {
			if err == nil {
				t.Fatal("failed chunk persisted")
			}
		} else if err != nil {
			t.Fatal("batch returned before durable data", i, err)
		}
		if m.state.ChunkLogs[h] != uint64(i+3) {
			t.Fatal("logical order lost", i)
		}
	}
	if !m.state.Buckets["before"] || !m.state.Buckets["after"] || m.state.Index != 11 {
		t.Fatal("non-chunk commands lost")
	}
}
