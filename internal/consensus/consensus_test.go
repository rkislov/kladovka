// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/storage"
)

func eventually(t *testing.T, limit time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("timed out waiting for consensus condition")
}

type fixture struct {
	nodes   []*Store
	cfgs    []*config.Config
	regs    []*cluster.Registry
	servers []*httptest.Server
	current []*atomic.Pointer[Store]
}

func tcpAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	peers := map[string]string{}
	for i := 0; i < 3; i++ {
		current := &atomic.Pointer[Store]{}
		ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s := current.Load()
			if s == nil || r.Header.Get("Authorization") != "Bearer consensus-test-token" {
				http.Error(w, "unavailable", 503)
				return
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/internal/consensus/chunk/{hash}", s.ChunkHandler)
			mux.ServeHTTP(w, r)
		}))
		ts.Start()
		f.servers = append(f.servers, ts)
		f.current = append(f.current, current)
		peers[cluster.NodeIDFromURL(ts.URL)] = tcpAddress(t)
	}
	for i, ts := range f.servers {
		cfg := &config.Config{ClusterMode: "consensus", RaftBootstrap: true, NodeID: cluster.NodeIDFromURL(ts.URL), NodeURL: ts.URL, DataRoot: t.TempDir(), ClusterToken: "consensus-test-token", RaftClusterID: "test-cluster", RaftPeers: peers, RaftListen: peers[cluster.NodeIDFromURL(ts.URL)], ReplicationFactor: 3, MaxObjectSize: 8 << 20, ClusterVerifyTLS: true}
		urls := []string{}
		for j, other := range f.servers {
			if j != i {
				urls = append(urls, other.URL)
			}
		}
		reg := cluster.NewRegistry(cfg.NodeID, cfg.NodeURL, urls, true, time.Second)
		reg.SetToken(cfg.ClusterToken)
		base, e := storage.New(cfg.DataRoot)
		if e != nil {
			t.Fatal(e)
		}
		s, e := Open(cfg, base, reg)
		if e != nil {
			t.Fatal(e)
		}
		f.nodes = append(f.nodes, s)
		f.cfgs = append(f.cfgs, cfg)
		f.regs = append(f.regs, reg)
		f.current[i].Store(s)
	}
	t.Cleanup(func() {
		for _, s := range f.nodes {
			s.Close()
		}
		for _, ts := range f.servers {
			ts.Close()
		}
	})
	f.leader(t)
	return f
}
func (f *fixture) leader(t *testing.T) int {
	t.Helper()
	leader := -1
	eventually(t, 10*time.Second, func() bool {
		for i, s := range f.nodes {
			if s.IsLeader() && s.Barrier() == nil {
				leader = i
				return true
			}
		}
		return false
	})
	return leader
}
func (f *fixture) restart(t *testing.T, i int) {
	t.Helper()
	f.current[i].Store(nil)
	f.nodes[i].Close()
	base, e := storage.New(f.cfgs[i].DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	s, e := Open(f.cfgs[i], base, f.regs[i])
	if e != nil {
		t.Fatal(e)
	}
	f.nodes[i] = s
	f.current[i].Store(s)
}
func TestRepairRestartFailoverAndTombstones(t *testing.T) {
	f := newFixture(t)
	leader := f.leader(t)
	s := f.nodes[leader]
	if e := s.CreatePolka("demo"); e != nil {
		t.Fatal(e)
	}
	original := bytes.Repeat([]byte("replication-data-"), 40000)
	meta, e := s.PutObject("demo", "file", bytes.NewReader(original), "text/plain")
	if e != nil {
		t.Fatal(e)
	}
	follower := (leader + 1) % 3
	eventually(t, 10*time.Second, func() bool {
		m, e := f.nodes[follower].Store.HeadObject("demo", "file")
		return e == nil && m.SHA256 == meta.SHA256
	})
	// Ensure old chunk entries are compacted: physical repair must use a peer,
	// not merely copy the original command from the local WAL.
	for i := 0; i < 40; i++ {
		if _, e = s.PutObject("demo", "history", strings.NewReader(strings.Repeat("x", i+1)), ""); e != nil {
			t.Fatal(e)
		}
	}
	eventually(t, 10*time.Second, func() bool { return f.nodes[follower].fsm.copyState().Index >= s.fsm.copyState().Index })
	if e = f.nodes[follower].raft.Snapshot().Error(); e != nil {
		t.Fatal(e)
	}
	record, _ := s.record("demo", "file")
	hash := record.Chunks[0].Hash
	chunkIndex := f.nodes[follower].fsm.copyState().ChunkLogs[hash]
	first, _ := f.nodes[follower].logs.FirstIndex()
	if first <= chunkIndex {
		t.Fatalf("test did not compact original chunk log: %d <= %d", first, chunkIndex)
	}
	if e = os.WriteFile(f.nodes[follower].fsm.path(hash), []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	eventually(t, 15*time.Second, func() bool {
		b, e := f.nodes[follower].fsm.chunk(hash)
		return e == nil && bytes.Equal(b, original[:ChunkSize])
	})
	// A process restart replays its persisted repair state and metadata.
	f.restart(t, follower)
	eventually(t, 10*time.Second, func() bool {
		r, e := f.nodes[follower].record("demo", "file")
		return e == nil && r.Meta.SHA256 == meta.SHA256
	})
	// Isolate only Raft transport: the old process and its HTTP endpoint remain up.
	old := f.nodes[leader]
	old.transport.Close()
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
	if _, e = old.PutObject("demo", "split-brain", strings.NewReader("forbidden"), ""); !errors.Is(e, ErrUnavailable) {
		t.Fatalf("isolated old leader accepted a write: %v", e)
	}
	if _, _, e = old.GetObject("demo", "file"); !errors.Is(e, ErrUnavailable) {
		t.Fatalf("isolated leader served a stale read: %v", e)
	}
	current := f.nodes[replacement]
	body, _, e := current.GetObject("demo", "file")
	if e != nil {
		t.Fatal(e)
	}
	data, _ := io.ReadAll(body)
	body.Close()
	if !bytes.Equal(data, original) {
		t.Fatal("acknowledged object lost after leader election")
	}
	if e = current.DeleteObject("demo", "file"); e != nil {
		t.Fatal(e)
	}
	if _, e = current.PutObject("demo", "new", strings.NewReader("after failover"), ""); e != nil {
		t.Fatal(e)
	}
	f.restart(t, leader)
	eventually(t, 15*time.Second, func() bool {
		st := f.nodes[leader].fsm.copyState()
		r, ok := st.Records["demo/file"]
		n := st.Records["demo/new"]
		return ok && r.Deleted && n.Meta.Size == 14
	})
	eventually(t, 10*time.Second, func() bool {
		_, e := f.nodes[leader].Store.HeadObject("demo", "file")
		return errors.Is(e, storage.ErrObjectNotFound)
	})
	// Lose quorum entirely. No surviving node may invent a new writable cluster.
	last := f.leader(t)
	for i, n := range f.nodes {
		if i != last {
			n.transport.Close()
		}
	}
	if _, e = f.nodes[last].PutObject("demo", "no-quorum", strings.NewReader("no"), ""); !errors.Is(e, ErrUnavailable) {
		t.Fatalf("write without majority: %v", e)
	}
}
func TestSnapshotRestoresDataAfterFullRestart(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	if e := s.CreatePolka("snap"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.PutObject("snap", "kept", strings.NewReader("snapshot body"), ""); e != nil {
		t.Fatal(e)
	}
	if e := s.DeleteObject("snap", "gone"); e != nil {
		t.Fatal(e)
	}
	for _, n := range f.nodes {
		eventually(t, 8*time.Second, func() bool { return n.fsm.copyState().Index >= s.fsm.copyState().Index })
		if e := n.raft.Snapshot().Error(); e != nil {
			t.Fatal(e)
		}
	}
	for i, n := range f.nodes {
		f.current[i].Store(nil)
		n.Close()
	}
	for i := range f.nodes {
		f.restart(t, i)
	}
	current := f.nodes[f.leader(t)]
	r, _, e := current.GetObject("snap", "kept")
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "snapshot body" {
		t.Fatal("snapshot lost content")
	}
	if _, _, e := current.GetObject("snap", "gone"); !errors.Is(e, storage.ErrObjectNotFound) {
		t.Fatalf("deleted object resurrected: %v", e)
	}
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("interrupted upload") }
func TestInterruptedStreamDoesNotPublishPartialObject(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	if e := s.CreatePolka("demo"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.PutObject("demo", "same", strings.NewReader("old committed value"), ""); e != nil {
		t.Fatal(e)
	}
	broken := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("p"), ChunkSize)), failedReader{})
	if _, e := s.PutObject("demo", "same", broken, ""); e == nil {
		t.Fatal("interrupted upload succeeded")
	}
	r, _, e := s.GetObject("demo", "same")
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old committed value" {
		t.Fatal("partial upload replaced committed data")
	}
	for i, n := range f.nodes {
		f.current[i].Store(nil)
		n.Close()
	}
	for i := range f.nodes {
		f.restart(t, i)
	}
	current := f.nodes[f.leader(t)]
	r, _, e = current.GetObject("demo", "same")
	if e != nil {
		t.Fatal(e)
	}
	b, _ = io.ReadAll(r)
	r.Close()
	if string(b) != "old committed value" {
		t.Fatal("partial write published after replay")
	}
}

func TestPruneRetainsSnapshotAndLiveReferences(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	if e := s.CreatePolka("gc"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.PutObject("gc", "file", strings.NewReader("old generation"), ""); e != nil {
		t.Fatal(e)
	}
	old, _ := s.record("gc", "file")
	held, _, e := s.GetObject("gc", "file")
	if e != nil {
		t.Fatal(e)
	}
	snap, e := s.fsm.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.PutObject("gc", "file", strings.NewReader("new generation"), ""); e != nil {
		t.Fatal(e)
	}
	if e = s.apply(command{Kind: "prune"}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.fsm.chunk(old.Chunks[0].Hash); e != nil {
		t.Fatal("prune removed a pinned snapshot chunk")
	}
	snap.Release()
	if _, e = s.fsm.chunk(old.Chunks[0].Hash); e != nil {
		t.Fatal("prune removed an active reader chunk")
	}
	oldBytes, readErr := io.ReadAll(held)
	held.Close()
	if readErr != nil || string(oldBytes) != "old generation" {
		t.Fatal("active reader lost its committed generation")
	}
	if _, e = os.Stat(s.fsm.path(old.Chunks[0].Hash)); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("unused snapshot generation not reclaimed")
	}
	r, _, e := s.GetObject("gc", "file")
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "new generation" {
		t.Fatal("prune damaged live data")
	}
}

func TestRejectMembershipResetAndLostStableState(t *testing.T) {
	f := newFixture(t)
	index := f.leader(t)
	if e := f.nodes[index].CreatePolka("snapshot-test"); e != nil {
		t.Fatal(e)
	}
	if e := f.nodes[index].raft.Snapshot().Error(); e != nil {
		t.Fatal(e)
	}
	f.current[index].Store(nil)
	f.nodes[index].Close()
	original := f.cfgs[index]
	changed := *original
	changed.RaftClusterID = "another-group"
	if node, e := Open(&changed, f.nodes[index].Store, f.regs[index]); e == nil {
		node.Close()
		t.Fatal("different cluster identity accepted")
	}
	root := original.DataRoot + "/.consensus"
	if e := os.Rename(root+"/raft.db", root+"/raft.db.saved"); e != nil {
		t.Fatal(e)
	}
	if node, e := Open(original, f.nodes[index].Store, f.regs[index]); e == nil {
		node.Close()
		t.Fatal("snapshot accepted without durable term/vote state")
	}
	if _, e := os.Stat(root + "/raft.db"); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("failed startup recreated lost term/vote database")
	}
	if e := os.Rename(root+"/raft.db.saved", root+"/raft.db"); e != nil {
		t.Fatal(e)
	}
	saved := root + ".saved"
	if e := os.Rename(root, saved); e != nil {
		t.Fatal(e)
	}
	if node, e := Open(original, f.nodes[index].Store, f.regs[index]); e == nil {
		node.Close()
		t.Fatal("lost stable state was bootstrapped again")
	}
	if _, e := os.Stat(root); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("failed startup recreated a lost-state directory")
	}
	if e := os.Rename(saved, root); e != nil {
		t.Fatal(e)
	}
	f.restart(t, index)
	fresh := *original
	fresh.DataRoot = t.TempDir()
	fresh.RaftBootstrap = false
	base, e := storage.New(fresh.DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	if node, e := Open(&fresh, base, f.regs[index]); e == nil {
		node.Close()
		t.Fatal("implicit new bootstrap accepted")
	}
	fresh.RaftBootstrap = true
	if e := base.CreatePolka("old-legacy-data"); e != nil {
		t.Fatal(e)
	}
	if node, e := Open(&fresh, base, f.regs[index]); e == nil {
		node.Close()
		t.Fatal("legacy data imported without migration")
	}
}
func TestConsensusQuotaIncludesMultipartParts(t *testing.T) {
	f := newFixture(t)
	s := f.nodes[f.leader(t)]
	s.cfg.StorageQuotaBytes = 12
	if e := s.CreatePolka("quota"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.PutObject("quota", "old", strings.NewReader("12345"), ""); e != nil {
		t.Fatal(e)
	}
	id, e := s.CreateMultipartUpload("quota", "multi", "")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.UploadPart(id, 1, strings.NewReader("67890")); e != nil {
		t.Fatal(e)
	}
	used, total, e := s.QuotaUsage()
	if e != nil || used != 10 || total != 12 {
		t.Fatalf("quota: %d %d %v", used, total, e)
	}
	if _, e = s.PutObject("quota", "extra", strings.NewReader("abc"), ""); !errors.Is(e, storage.ErrQuotaExceeded) {
		t.Fatalf("multipart quota bypass: %v", e)
	}
	if _, e = s.UploadPart(id, 2, strings.NewReader("abc")); !errors.Is(e, storage.ErrQuotaExceeded) {
		t.Fatalf("part quota bypass: %v", e)
	}
	if e = s.AbortMultipartUpload(id); e != nil {
		t.Fatal(e)
	}
	if _, e = s.PutObject("quota", "extra", strings.NewReader("abc"), ""); e != nil {
		t.Fatal(e)
	}
}
