// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/storage"
)

func TestRepairQueueFairnessAndBackoff(t *testing.T) {
	var q repairQueue
	q.add("z", 1)
	q.add("m", 2)
	w := q.order.Front().Value.(*repairWork)
	now := time.Now()
	for i := 0; i < 10; i++ {
		w.failed(errors.New("disk full"), now)
	}
	if w.delay != time.Minute || !w.retry.Equal(now.Add(time.Minute)) {
		t.Fatal("unbounded retry", w)
	}
	q.add("a", 3)
	q.add("z", 4)
	if q.order.Len() != 3 || q.order.Front().Value.(*repairWork).key != "z" || w.err != "" || !w.retry.IsZero() {
		t.Fatal("generation reset moved or duplicated work")
	}
	if nextRepairKey([]string{"a", "b", "m", "z"}, "m") != "z" || nextRepairKey([]string{"a", "z"}, "z") != "a" {
		t.Fatal("cursor displaced by insertions")
	}
}

func TestMaterializationBatchFailureDoesNotStarveAndUpdatesMetadata(t *testing.T) {
	base, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &machine{state: emptyState()}
	s := &Store{Store: base, fsm: m, repairIndex: map[string]uint64{}}
	sum := sha256.Sum256(nil)
	hash := hex.EncodeToString(sum[:])
	m.state.Buckets["demo"] = true
	record := func(key string, index uint64) Record {
		return Record{Polka: "demo", Key: key, Index: index, Meta: storage.ObjectMeta{Key: key, SHA256: hash, ContentType: "text/plain", ETag: "original"}}
	}
	// A malformed cache path fails deterministically without relying on permissions/root.
	bad := record("bad", 1)
	bad.Polka = "../invalid"
	m.state.Records["bad"] = bad
	var q repairQueue
	q.add("bad", 1)
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("demo/%02d", i)
		m.state.Records[key] = record(fmt.Sprintf("%02d", i), uint64(i+2))
		q.add(key, uint64(i+2))
	}
	eventually(t, 10*time.Second, func() bool { s.materializeBatch(context.Background(), &q); return q.order.Len() == 1 })
	if q.entries["bad"].Value.(*repairWork).err == "" || len(s.repairIndex) != 12 {
		t.Fatal("failure swallowed or healthy work starved")
	}
	r := m.state.Records["demo/00"]
	r.Index = 99
	r.Meta.ContentType = "application/json"
	r.Meta.ETag = "changed"
	m.state.Records["demo/00"] = r
	q.add("demo/00", r.Index)
	eventually(t, time.Second, func() bool { s.materializeBatch(context.Background(), &q); return s.repairIndex["demo/00"] == 99 })
	meta, err := base.HeadObject("demo", "00")
	if err != nil || meta.ETag != "changed" || meta.ContentType != "application/json" {
		t.Fatal(meta, err)
	}
	r.Deleted = true
	r.Index = 100
	m.state.Records["demo/00"] = r
	q.add("demo/00", 100)
	eventually(t, time.Second, func() bool { s.materializeBatch(context.Background(), &q); return s.repairIndex["demo/00"] == 100 })
	if _, err = base.HeadObject("demo", "00"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("tombstone not materialized", err)
	}
	// Restart loses only derived acknowledgements; every record becomes pending again.
	s.repairIndex = map[string]uint64{}
	q = repairQueue{}
	cursor := ""
	s.refreshRepairQueue(&q, &cursor)
	if q.order.Len() != len(m.state.Records) {
		t.Fatal("restart skipped records")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.materializeBatch(ctx, &q)
	if len(s.repairIndex) != 0 {
		t.Fatal("canceled batch performed IO")
	}
}

func TestMaterializationFullCatchupAfterOfflineWritesAndRestart(t *testing.T) {
	f := newFixture(t)
	leader := f.leader(t)
	s := f.nodes[leader]
	if err := s.CreatePolka("catchup"); err != nil {
		t.Fatal(err)
	}
	offline := (leader + 1) % 3
	f.current[offline].Store(nil)
	f.nodes[offline].Close()
	expected := map[string][]byte{}
	for i := 0; i < 48; i++ {
		// Descending keys expose offset-cursor starvation during catalogue growth.
		key := fmt.Sprintf("item-%03d", 48-i)
		body := []byte(fmt.Sprintf("unique-body-%d", i))
		if _, err := s.PutObject("catchup", key, bytes.NewReader(body), "text/plain"); err != nil {
			t.Fatal(err)
		}
		expected[key] = body
	}
	if _, err := s.PutObject("catchup", "item-001", bytes.NewReader([]byte("overwritten")), "application/json"); err != nil {
		t.Fatal(err)
	}
	expected["item-001"] = []byte("overwritten")
	if err := s.DeleteObject("catchup", "item-002"); err != nil {
		t.Fatal(err)
	}
	delete(expected, "item-002")
	s.fsm.mu.RLock()
	target := s.fsm.state.Index
	s.fsm.mu.RUnlock()
	f.restart(t, offline)
	check := func() bool {
		for _, node := range f.nodes {
			status := node.Status()
			if status["applied_index"].(uint64) < target || status["repair_pending"].(int) != 0 || status["repair_error"] != "" {
				return false
			}
			for key, want := range expected {
				got, err := os.ReadFile(filepath.Join(node.cfg.DataRoot, "catchup", key))
				if err != nil || !bytes.Equal(got, want) {
					return false
				}
			}
			if _, err := os.Stat(filepath.Join(node.cfg.DataRoot, "catchup", "item-002")); !os.IsNotExist(err) {
				return false
			}
		}
		return true
	}
	eventually(t, 30*time.Second, check)
	// Freeze only this replica's cache, then commit a new generation. Status must
	// count the new FSM record even before the worker's next queue refresh.
	node := f.nodes[offline]
	func() {
		node.cacheMu.Lock()
		defer node.cacheMu.Unlock()
		body := []byte("new generation while cache paused")
		if _, err := s.PutObject("catchup", "item-001", bytes.NewReader(body), "text/plain"); err != nil {
			t.Fatal(err)
		}
		expected["item-001"] = body
		s.fsm.mu.RLock()
		target = s.fsm.state.Index
		s.fsm.mu.RUnlock()
		eventually(t, 5*time.Second, func() bool { node.fsm.mu.RLock(); defer node.fsm.mu.RUnlock(); return node.fsm.state.Index >= target })
		if node.Status()["repair_pending"].(int) == 0 {
			t.Fatal("new generation reported as materialized")
		}
	}()
	eventually(t, 30*time.Second, check)
	// Remove a materialized file while this follower is stopped. Restart must rebuild it.
	f.current[offline].Store(nil)
	f.nodes[offline].Close()
	if err := os.Remove(filepath.Join(f.cfgs[offline].DataRoot, "catchup", "item-001")); err != nil {
		t.Fatal(err)
	}
	f.restart(t, offline)
	eventually(t, 30*time.Second, check)
}
