// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package consensus

import (
	"bytes"
	"errors"
	"github.com/rkislov/kladovka/internal/storage"
	"io"
	"strings"
	"testing"
	"time"
)

func TestVersionsSurviveFailoverSnapshotAndReplicaCatchup(t *testing.T) {
	f := newFixture(t)
	for _, cfg := range f.cfgs {
		cfg.VersioningEnabled = true
	}
	leader := f.leader(t)
	s := f.nodes[leader]
	if e := s.CreatePolka("history"); e != nil {
		t.Fatal(e)
	}
	original := bytes.Repeat([]byte("old generation"), 30000)
	first, e := s.PutObject("history", "x", bytes.NewReader(original), "text/plain")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.PutObject("history", "x", strings.NewReader("new"), ""); e != nil {
		t.Fatal(e)
	}
	marker, e := s.TrashObject("history", "x")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.apply(command{Kind: "prune"}); e != nil {
		t.Fatal(e)
	}
	boundary := s.fsm.copyState().Index
	for _, n := range f.nodes {
		eventually(t, 10*time.Second, func() bool { return n.fsm.copyState().Index >= boundary })
		if e = n.raft.Snapshot().Error(); e != nil {
			t.Fatal(e)
		}
	}
	// Isolate the acknowledged leader, then restore through the new quorum.
	s.transport.Close()
	replacement := -1
	eventually(t, 10*time.Second, func() bool {
		for i, n := range f.nodes {
			if i != leader && n.IsLeader() && n.Barrier() == nil {
				replacement = i
				return true
			}
		}
		return false
	})
	if _, _, e = s.GetVersion("history", "x", first.VersionID); !errors.Is(e, ErrUnavailable) {
		t.Fatal("isolated leader served version", e)
	}
	s = f.nodes[replacement]
	r, m, e := s.GetVersion("history", "x", first.VersionID)
	if e != nil {
		t.Fatal(e)
	}
	data, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(data, original) {
		t.Fatal("historical bytes lost")
	}
	restored, e := s.PutObjectWithETag("history", "x", bytes.NewReader(data), m.ContentType, m.ETag)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ExpireTrash("history", "x", marker.VersionID); e != nil {
		t.Fatal(e)
	}
	// Restart former leader from durable snapshot/WAL and wait for all generations.
	f.restart(t, leader)
	boundary = s.fsm.copyState().Index
	for _, n := range f.nodes {
		eventually(t, 15*time.Second, func() bool { return n.fsm.copyState().Index >= boundary })
		st := n.fsm.copyState()
		versions := st.Versions["history/x"]
		if len(versions) != 4 || st.Records["history/x"].Meta.VersionID != restored.VersionID {
			t.Fatal("replica history differs", len(versions))
		}
		for _, v := range versions {
			for _, c := range v.Chunks {
				eventually(t, 10*time.Second, func() bool { _, err := n.fsm.chunk(c.Hash); return err == nil })
			}
		}
	}
	s = f.nodes[f.leader(t)]
	marker, e = s.TrashObject("history", "x")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ExpireTrash("history", "x", marker.VersionID); e != nil {
		t.Fatal(e)
	}
	if e = s.apply(command{Kind: "prune"}); e != nil {
		t.Fatal(e)
	}
	boundary = s.fsm.copyState().Index
	for _, n := range f.nodes {
		eventually(t, 10*time.Second, func() bool { return n.fsm.copyState().Index >= boundary })
		st := n.fsm.copyState()
		if len(st.Versions["history/x"]) != 0 || !st.Records["history/x"].Deleted || len(st.Chunks) != 0 {
			t.Fatal("purged data remains live")
		}
	}
	f.restart(t, replacement)
	s = f.nodes[f.leader(t)]
	if _, _, e = s.GetVersion("history", "x", first.VersionID); !errors.Is(e, storage.ErrObjectNotFound) {
		t.Fatal("purged version resurrected", e)
	}
}
