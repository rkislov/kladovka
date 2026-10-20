// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/storage"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func (f *fixture) newMember(t *testing.T) *Store {
	t.Helper()
	current := &atomic.Pointer[Store]{}
	ready := &atomic.Bool{}
	ready.Store(true)
	f.ready = append(f.ready, ready)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := current.Load()
		if s == nil || r.Header.Get("Authorization") != "Bearer consensus-test-token" {
			http.Error(w, "unavailable", 503)
			return
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/internal/consensus/candidate", s.CandidateHandler)
		mux.HandleFunc("/internal/consensus/ready", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("verify") == "true" && !ready.Load() {
				http.Error(w, "not ready", 503)
				return
			}
			s.ReadyHandler(w, r)
		})
		mux.HandleFunc("/internal/consensus/chunk/{hash}", s.ChunkHandler)
		mux.ServeHTTP(w, r)
	}))
	cfg := *f.cfgs[0]
	cfg.NodeURL = ts.URL
	cfg.NodeID = cluster.NodeIDFromURL(ts.URL)
	cfg.RaftBootstrap = false
	cfg.RaftJoin = true
	cfg.RaftReplacementAddr = tcpAddress(t)
	cfg.RaftListen = cfg.RaftReplacementAddr
	cfg.DataRoot = t.TempDir()
	urls := []string{}
	for _, v := range f.servers {
		urls = append(urls, v.URL)
	}
	reg := cluster.NewRegistry(cfg.NodeID, cfg.NodeURL, urls, true, time.Second)
	reg.SetToken(cfg.ClusterToken)
	base, e := storage.New(cfg.DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	s, e := Open(&cfg, base, reg)
	if e != nil {
		t.Fatal(e)
	}
	current.Store(s)
	f.nodes = append(f.nodes, s)
	f.current = append(f.current, current)
	f.cfgs = append(f.cfgs, &cfg)
	f.regs = append(f.regs, reg)
	f.servers = append(f.servers, ts)
	return s
}
func TestMembershipExpandAndRemoveLeader(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	if e := s.CreatePolka("before-join"); e != nil {
		t.Fatal(e)
	}
	if e := s.ChangeMembership("too-small", "remove", "", s.cfg.NodeID, ""); e == nil {
		t.Fatal("removed from three voters")
	}
	for _, id := range []string{"fourth", "fifth"} {
		n := f.newMember(t)
		if n.CanRoute() {
			t.Fatal("unadmitted node routes requests")
		}
		if e := s.ChangeMembership(id, "add", n.cfg.NodeURL, "", string(n.localID)); e != nil {
			t.Fatal(e)
		}
		if e := s.ChangeMembership(id, "add", n.cfg.NodeURL, "", string(n.localID)); e != nil {
			t.Fatal("retry", e)
		}
		eventually(t, 20*time.Second, func() bool { c := s.membershipChange(); return c != nil && c.Done })
		if !n.CanRoute() {
			t.Fatal("admitted member cannot route")
		}
	}
	if s.voterCount() != 5 || s.TargetRF() != 5 {
		t.Fatal("expected five replicas")
	}
	removed := s.cfg.NodeID
	if e := s.ChangeMembership("remove-leader", "remove", "", removed, ""); e != nil {
		t.Fatal(e)
	}
	eventually(t, 20*time.Second, func() bool {
		for _, n := range f.nodes {
			if n.IsLeader() {
				c := n.membershipChange()
				if c != nil && c.ID == "remove-leader" && c.Done {
					return true
				}
			}
		}
		return false
	})
	s = f.nodes[f.leader(t)]
	if s.cfg.NodeID == removed || s.voterCount() != 4 || s.TargetRF() != 4 {
		t.Fatal("leader removal incomplete")
	}
	if e := s.ChangeMembership("remove-leader", "remove", "", removed, ""); e != nil {
		t.Fatal("completed retry", e)
	}
	if e := s.ChangeMembership("remove-leader", "remove", "", s.cfg.NodeID, ""); e == nil {
		t.Fatal("accepted conflicting request ID")
	}
	target := f.nodes[4].cfg.NodeID
	if target == s.cfg.NodeID {
		target = f.nodes[3].cfg.NodeID
	}
	if e := s.ChangeMembership("back-to-three", "remove", "", target, ""); e != nil {
		t.Fatal(e)
	}
	eventually(t, 20*time.Second, func() bool { c := s.membershipChange(); return c != nil && c.Done })
	if s.voterCount() != 3 || s.TargetRF() != 3 {
		t.Fatal("expected three replicas")
	}
	for i, n := range f.nodes {
		if n.cfg.NodeID != removed && n.cfg.NodeID != target {
			f.restart(t, i)
		}
	}
	s = f.nodes[f.leader(t)]
	if s.TargetRF() != 3 || s.nodeURL(removed) != "" || s.nodeURL(target) != "" {
		t.Fatal("topology not durable")
	}
	if e := s.CreatePolka("after-restart"); e != nil {
		t.Fatal(e)
	}
}

func TestMembershipPendingSurvivesRestart(t *testing.T) {
	f := newFixture(t)
	li := f.leader(t)
	s := f.nodes[li]
	n := f.newMember(t)
	f.ready[3].Store(false)
	if e := s.ChangeMembership("durable-add", "add", n.cfg.NodeURL, "", string(n.localID)); e != nil {
		t.Fatal(e)
	}
	eventually(t, 10*time.Second, func() bool {
		for _, m := range s.members() {
			if m.RaftID == string(n.localID) && m.Suffrage == "Nonvoter" {
				return true
			}
		}
		return false
	})
	if s.membershipChange().Done || s.voterCount() != 3 {
		t.Fatal("unverified learner promoted")
	}
	f.restart(t, 3)
	f.restart(t, li)
	s = f.nodes[f.leader(t)]
	if s.membershipChange() == nil || s.membershipChange().ID != "durable-add" {
		t.Fatal("intent lost")
	}
	f.ready[3].Store(true)
	eventually(t, 20*time.Second, func() bool { c := s.membershipChange(); return c != nil && c.Done })
	if s.voterCount() != 4 {
		t.Fatal("learner not promoted")
	}
	if e := s.raft.Snapshot().Error(); e != nil {
		t.Fatal(e)
	}
	for i := range f.nodes {
		f.nodes[i].Close()
	}
	for i := range f.nodes {
		f.restart(t, i)
	}
	s = f.nodes[f.leader(t)]
	if s.TargetRF() != 4 || s.nodeURL(n.cfg.NodeID) != n.cfg.NodeURL {
		t.Fatal("snapshot lost topology")
	}
}
