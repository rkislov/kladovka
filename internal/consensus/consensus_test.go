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

	"github.com/hashicorp/raft"
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
	ready   []*atomic.Bool
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
		ready := &atomic.Bool{}
		ready.Store(true)
		f.ready = append(f.ready, ready)
		ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s := current.Load()
			if s == nil || r.Header.Get("Authorization") != "Bearer consensus-test-token" {
				http.Error(w, "unavailable", 503)
				return
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/internal/consensus/chunk/{hash}", s.ChunkHandler)
			mux.HandleFunc("/internal/consensus/candidate", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("verify") == "true" && !ready.Load() {
					http.Error(w, "not ready", 503)
					return
				}
				s.CandidateHandler(w, r)
			})
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
	changed = *original
	changed.NodeID = f.cfgs[(index+1)%3].NodeID
	if node, e := Open(&changed, f.nodes[index].Store, f.regs[index]); e == nil {
		node.Close()
		t.Fatal("durable voter state reused under another node identity")
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

func TestReplaceLostNodeUsesFreshIdentityAndLearner(t *testing.T) {
	f := newFixture(t)
	leader := f.leader(t)
	s := f.nodes[leader]
	if e := s.CreatePolka("replacement"); e != nil {
		t.Fatal(e)
	}
	payload := bytes.Repeat([]byte("durable-replacement"), 30000)
	if _, e := s.PutObject("replacement", "kept", bytes.NewReader(payload), ""); e != nil {
		t.Fatal(e)
	}
	if _, e := s.PutObject("replacement", "deleted", strings.NewReader("old"), ""); e != nil {
		t.Fatal(e)
	}
	if e := s.DeleteObject("replacement", "deleted"); e != nil {
		t.Fatal(e)
	}
	for n := 0; n < 40; n++ {
		if _, e := s.PutObject("replacement", "history", strings.NewReader(strings.Repeat("x", n+1)), ""); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.raft.Snapshot().Error(); e != nil {
		t.Fatal(e)
	}
	index := (leader + 1) % 3
	original := *f.cfgs[index]
	f.current[index].Store(nil)
	f.nodes[index].Close()
	replacement := original
	replacement.DataRoot = t.TempDir()
	replacement.RaftBootstrap = false
	replacement.RaftJoin = true
	replacement.RaftReplacementAddr = tcpAddress(t)
	replacement.RaftListen = replacement.RaftReplacementAddr
	f.cfgs[index] = &replacement
	f.ready[index].Store(false)
	base, e := storage.New(replacement.DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	candidate, e := Open(&replacement, base, f.regs[index])
	if e != nil {
		t.Fatal(e)
	}
	f.nodes[index] = candidate
	f.current[index].Store(candidate)
	if candidate.IsLeader() || candidate.localID == s.localID || candidate.localID == raft.ServerID(original.NodeID) {
		t.Fatal("fresh candidate reused voter identity")
	}
	if candidate.Barrier() == nil {
		t.Fatal("unadmitted candidate acquired a quorum")
	}
	if e = s.Replace(original.NodeID, string(candidate.localID)); e != nil {
		t.Fatal(e)
	}
	if e = s.Replace(original.NodeID, string(candidate.localID)); e != nil {
		t.Fatal("retry must be idempotent", e)
	}
	eventually(t, 15*time.Second, func() bool {
		for _, m := range s.members() {
			if m.RaftID == string(candidate.localID) {
				return m.Suffrage == raft.Nonvoter.String()
			}
		}
		return false
	})
	if s.voterCount() != 2 {
		t.Fatal("learner voted before verification")
	}
	// A waiting learner survives both its restart and a full leader-process restart.
	id := candidate.localID
	f.restart(t, index)
	candidate = f.nodes[index]
	if candidate.localID != id {
		t.Fatal("restart changed candidate incarnation")
	}
	f.restart(t, leader)
	leader = f.leader(t)
	s = f.nodes[leader]
	if r := s.replacement(); r == nil || r.Done {
		t.Fatal("durable replacement intent lost")
	}
	f.ready[index].Store(true)
	eventually(t, 20*time.Second, func() bool { r := s.replacement(); return r != nil && r.Done })
	if s.voterCount() != 3 {
		t.Fatal("replacement did not restore voter count")
	}
	if e = s.raft.LeadershipTransferToServer(candidate.localID, raft.ServerAddress(replacement.RaftReplacementAddr)).Error(); e != nil {
		t.Fatal(e)
	}
	eventually(t, 10*time.Second, func() bool { return candidate.IsLeader() && candidate.Barrier() == nil })
	if candidate.LeaderURL() != replacement.NodeURL {
		t.Fatal("fresh leader did not map to its HTTP slot")
	}
	if e = candidate.raft.LeadershipTransferToServer(s.localID, raft.ServerAddress(s.cfg.RaftPeers[s.cfg.NodeID])).Error(); e != nil {
		t.Fatal(e)
	}
	eventually(t, 10*time.Second, func() bool { return s.IsLeader() && s.Barrier() == nil })
	if e = s.Barrier(); e != nil {
		t.Fatal(e)
	}
	// With one original voter stopped, the replacement must participate in quorum.
	other := 3 - leader - index
	f.current[other].Store(nil)
	f.nodes[other].Close()
	if _, e = s.PutObject("replacement", "after", strings.NewReader("new quorum"), ""); e != nil {
		t.Fatal(e)
	}
	r, _, e := s.GetObject("replacement", "kept")
	if e != nil {
		t.Fatal(e)
	}
	body, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(body, payload) {
		t.Fatal("acknowledged content lost during replacement")
	}
	if _, _, e = s.GetObject("replacement", "deleted"); !errors.Is(e, storage.ErrObjectNotFound) {
		t.Fatal("deleted object resurrected", e)
	}
	// The retired voter may return with its original disk/port but must not form
	// a second quorum or accept writes. It never shares the fresh Raft endpoint.
	oldBase, e := storage.New(original.DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	old, e := Open(&original, oldBase, f.regs[index])
	if e != nil {
		t.Fatal(e)
	}
	defer old.Close()
	time.Sleep(2 * time.Second)
	if old.Barrier() == nil {
		t.Fatal("retired incarnation acquired a quorum")
	}
	if _, e = old.PutObject("replacement", "stale", strings.NewReader("bad"), ""); e == nil {
		t.Fatal("retired incarnation accepted a write")
	}
	// No second replacement without the two surviving votes.
	f.current[index].Store(nil)
	candidate.Close()
	if e = s.Replace(original.NodeID, string(id)); e == nil {
		t.Fatal("replacement accepted without quorum")
	}
}

func TestFreshCandidateCannotAcknowledgeRetiredEndpoint(t *testing.T) {
	f := newFixture(t)
	leader := f.leader(t)
	s := f.nodes[leader]
	if e := s.CreatePolka("isolation"); e != nil {
		t.Fatal(e)
	}
	for i := range f.nodes {
		if i != leader {
			f.current[i].Store(nil)
			f.nodes[i].Close()
		}
	}
	index := (leader + 1) % 3
	cfg := *f.cfgs[index]
	cfg.DataRoot = t.TempDir()
	cfg.RaftBootstrap = false
	cfg.RaftJoin = true
	cfg.RaftReplacementAddr = tcpAddress(t)
	cfg.RaftListen = cfg.RaftReplacementAddr
	base, e := storage.New(cfg.DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	candidate, e := Open(&cfg, base, f.regs[index])
	if e != nil {
		t.Fatal(e)
	}
	f.nodes[index] = candidate
	f.current[index].Store(candidate)
	if _, e = s.PutObject("isolation", "unconfirmed", strings.NewReader("must fail"), ""); e == nil {
		t.Fatal("isolated old leader counted a fresh candidate as the old voter")
	}
	if e = s.Replace(cfg.NodeID, string(candidate.localID)); e == nil {
		t.Fatal("replacement started without the old majority")
	}
	if names, e := candidate.ListPolki(); e != nil || len(names) != 0 {
		t.Fatalf("unadmitted candidate received the old endpoint's log: %v %v", names, e)
	}
}

func TestJoinRetryAfterTransportConfigurationFailure(t *testing.T) {
	f := newFixture(t)
	cfg := *f.cfgs[0]
	cfg.DataRoot = t.TempDir()
	cfg.RaftBootstrap = false
	cfg.RaftJoin = true
	cfg.RaftReplacementAddr = tcpAddress(t)
	cfg.RaftListen = cfg.RaftReplacementAddr
	cfg.RaftTLSCert = "/no-such-test-certificate"
	cfg.RaftTLSKey = "/no-such-test-key"
	cfg.RaftTLSCA = "/no-such-test-ca"
	base, e := storage.New(cfg.DataRoot)
	if e != nil {
		t.Fatal(e)
	}
	if node, e := Open(&cfg, base, f.regs[0]); e == nil {
		node.Close()
		t.Fatal("invalid TLS configuration accepted")
	}
	manifest, e := os.ReadFile(cfg.DataRoot + "/.consensus/identity")
	if e != nil {
		t.Fatal(e)
	}
	cfg.RaftTLSCert = ""
	cfg.RaftTLSKey = ""
	cfg.RaftTLSCA = ""
	node, e := Open(&cfg, base, f.regs[0])
	if e != nil {
		t.Fatal(e)
	}
	defer node.Close()
	if string(node.localID) != strings.Split(string(manifest), "\n")[1] {
		t.Fatal("retry changed durable joining identity")
	}
}
