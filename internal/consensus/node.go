// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/storage"
)

type Store struct {
	*storage.Store
	cfg              *config.Config
	reg              *cluster.Registry
	fsm              *machine
	raft             *raft.Raft
	transport        *raft.NetworkTransport
	logs             *bolt.BoltStore
	writeMu          sync.Mutex
	cacheMu          sync.Mutex
	ctx              context.Context
	repairMu         sync.Mutex
	repairIndex      map[string]uint64
	pending          int
	repairError      string
	replacementError string
	membershipError  string
	cancel           context.CancelFunc
	done             chan struct{}
	closeOnce        sync.Once
	closeErr         error
	localID          raft.ServerID
}

func Open(cfg *config.Config, base *storage.Store, reg *cluster.Registry) (*Store, error) {
	root := filepath.Join(cfg.DataRoot, ".consensus")
	_, statErr := os.Stat(root)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if errors.Is(statErr, os.ErrNotExist) {
		if _, e := os.Lstat(filepath.Join(cfg.DataRoot, ".kladovka-consensus")); !errors.Is(e, os.ErrNotExist) {
			return nil, errors.New("consensus state was lost; restore saved raft state")
		}
		if !cfg.RaftBootstrap && !cfg.RaftJoin {
			return nil, errors.New("new consensus node requires explicit RAFT_BOOTSTRAP or RAFT_JOIN")
		}
		names, e := base.ListPolki()
		if e != nil {
			return nil, e
		}
		if len(names) > 0 {
			return nil, errors.New("consensus requires a new empty DATA_ROOT; migrate existing data through S3")
		}
	}
	if e := os.MkdirAll(filepath.Join(root, "chunks"), 0700); e != nil {
		return nil, e
	}
	identity, _ := json.Marshal(struct {
		Version         int
		ID              string
		NodeID          string
		Peers           map[string]string
		RF              int
		ReplacementAddr string `json:",omitempty"`
	}{1, cfg.RaftClusterID, cfg.NodeID, cfg.RaftPeers, cfg.ReplicationFactor, cfg.RaftReplacementAddr})
	fingerprint := sha256.Sum256(identity)
	manifest := []byte(hex.EncodeToString(fingerprint[:]))
	modePath := filepath.Join(cfg.DataRoot, ".kladovka-consensus")
	modePersisted := false
	localID := cfg.NodeID
	if old, e := os.ReadFile(modePath); e == nil {
		parts := strings.Split(string(old), "\n")
		if len(parts) > 2 || parts[0] != string(manifest) {
			return nil, errors.New("consensus cluster identity differs")
		}
		if len(parts) == 2 {
			if !validIncarnation(cfg.NodeID, parts[1]) {
				return nil, errors.New("invalid persisted raft incarnation")
			}
			localID = parts[1]
			manifest = old
		}
		modePersisted = true
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	if !modePersisted && cfg.RaftJoin {
		old, e := os.ReadFile(filepath.Join(root, "identity"))
		if e == nil {
			parts := strings.Split(string(old), "\n")
			if len(parts) != 2 || parts[0] != string(manifest) || !validIncarnation(cfg.NodeID, parts[1]) {
				return nil, errors.New("unfinished join identity differs")
			}
			info, statErr := os.Stat(filepath.Join(root, "raft.db"))
			if statErr != nil || !info.Mode().IsRegular() || info.Size() == 0 {
				return nil, errors.New("unfinished join lost durable raft state; use a new candidate")
			}
			localID = parts[1]
			manifest = old
		} else if errors.Is(e, os.ErrNotExist) {
			var nonce [16]byte
			if _, e := rand.Read(nonce[:]); e != nil {
				return nil, e
			}
			localID = cfg.NodeID + "#" + hex.EncodeToString(nonce[:])
			manifest = append(manifest, []byte("\n"+localID)...)
		} else {
			return nil, e
		}
	}
	_, bootstrapMarkerErr := os.Stat(filepath.Join(root, "bootstrapped"))
	if bootstrapMarkerErr != nil && !errors.Is(bootstrapMarkerErr, os.ErrNotExist) {
		return nil, bootstrapMarkerErr
	}
	if modePersisted || bootstrapMarkerErr == nil {
		info, e := os.Stat(filepath.Join(root, "raft.db"))
		if e != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return nil, errors.New("consensus stable term/vote database was lost; restore saved raft state, even if snapshots remain")
		}
	}
	path := filepath.Join(root, "identity")
	if old, e := os.ReadFile(path); e == nil {
		if string(old) != string(manifest) {
			return nil, errors.New("consensus cluster identity or static membership changed")
		}
	} else if errors.Is(e, os.ErrNotExist) {
		if e = os.WriteFile(path, manifest, 0600); e != nil {
			return nil, e
		}
		f, e := os.OpenFile(path, os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		e = f.Sync()
		f.Close()
		if e != nil {
			return nil, e
		}
		if e = syncDirectory(root); e != nil {
			return nil, e
		}
	} else {
		return nil, e
	}
	if e := syncDirectory(root); e != nil {
		return nil, e
	}
	if e := syncDirectory(cfg.DataRoot); e != nil {
		return nil, e
	}
	m := &machine{dir: filepath.Join(root, "chunks"), state: emptyState(), free: base.CheckFreeSpace}
	logs, e := bolt.NewBoltStore(filepath.Join(root, "raft.db"))
	if e != nil {
		return nil, e
	}
	fail := func(e error) (*Store, error) { logs.Close(); return nil, e }
	advertise := cfg.RaftPeers[cfg.NodeID]
	if cfg.RaftReplacementAddr != "" {
		advertise = cfg.RaftReplacementAddr
	}
	stream, e := newStream(cfg.RaftListen, advertise, cfg.RaftTLSCert, cfg.RaftTLSKey, cfg.RaftTLSCA, cfg.ClusterToken+"\x00"+cfg.RaftClusterID)
	if e != nil {
		return fail(e)
	}
	transport := raft.NewNetworkTransport(stream, 3, 5*time.Second, io.Discard)
	snapshots, e := raft.NewFileSnapshotStore(root, 2, io.Discard)
	if e != nil {
		transport.Close()
		return fail(e)
	}
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(localID)
	rc.LogOutput = io.Discard
	rc.HeartbeatTimeout = time.Second
	rc.ElectionTimeout = time.Second
	rc.LeaderLeaseTimeout = 500 * time.Millisecond
	rc.SnapshotInterval = 30 * time.Second
	rc.SnapshotThreshold = 128
	rc.TrailingLogs = 32
	rc.MaxAppendEntries = 8
	existing, e := raft.HasExistingState(logs, logs, snapshots)
	if e != nil {
		transport.Close()
		return fail(e)
	}
	// Every fresh voter uses exactly the same immutable initial configuration.
	// Never bootstrap a replacement independently or recover a minority by force.
	if !existing && localID == cfg.NodeID {
		_, markerErr := os.Stat(filepath.Join(root, "bootstrapped"))
		if modePersisted || !cfg.RaftBootstrap || !errors.Is(markerErr, os.ErrNotExist) {
			transport.Close()
			return fail(errors.New("empty raft state requires explicit RAFT_BOOTSTRAP on a new DATA_ROOT; restore lost raft state instead of rebootstrap"))
		}
		ids := make([]string, 0, len(cfg.RaftPeers))
		for id := range cfg.RaftPeers {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		membership := raft.Configuration{}
		for _, id := range ids {
			membership.Servers = append(membership.Servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(cfg.RaftPeers[id]), Suffrage: raft.Voter})
		}
		if e = raft.BootstrapCluster(rc, logs, logs, snapshots, transport, membership); e != nil {
			transport.Close()
			return fail(e)
		}
		if e = os.WriteFile(filepath.Join(root, "bootstrapped"), []byte("1\n"), 0600); e != nil {
			transport.Close()
			return fail(e)
		}
		marker, e := os.OpenFile(filepath.Join(root, "bootstrapped"), os.O_RDWR, 0600)
		if e != nil {
			transport.Close()
			return fail(e)
		}
		e = marker.Sync()
		marker.Close()
		if e != nil {
			transport.Close()
			return fail(e)
		}
		if e = syncDirectory(root); e != nil {
			transport.Close()
			return fail(e)
		}
	}
	if !modePersisted {
		if e = os.WriteFile(modePath, manifest, 0600); e != nil {
			transport.Close()
			return fail(e)
		}
		file, e := os.OpenFile(modePath, os.O_RDWR, 0600)
		if e != nil {
			transport.Close()
			return fail(e)
		}
		e = file.Sync()
		file.Close()
		if e != nil {
			transport.Close()
			return fail(e)
		}
		if e = syncDirectory(cfg.DataRoot); e != nil {
			transport.Close()
			return fail(e)
		}
	}
	r, e := raft.NewRaft(rc, m, logs, logs, snapshots, transport)
	if e != nil {
		transport.Close()
		return fail(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{Store: base, ctx: ctx, cfg: cfg, reg: reg, fsm: m, raft: r, transport: transport, logs: logs, repairIndex: map[string]uint64{}, cancel: cancel, done: make(chan struct{}), localID: raft.ServerID(localID)}
	go s.repairLoop(ctx)
	return s, nil
}
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
		s.closeErr = s.raft.Shutdown().Error()
		s.transport.Close()
		if e := s.logs.Close(); s.closeErr == nil {
			s.closeErr = e
		}
	})
	return s.closeErr
}
func (s *Store) LeaderURL() string {
	_, id := s.raft.LeaderWithID()
	return s.nodeURL(slotForID(string(id)))
}
func (s *Store) IsLeader() bool { return s.raft.State() == raft.Leader }
func (s *Store) Barrier() error {
	if e := s.raft.VerifyLeader().Error(); e != nil {
		return ErrUnavailable
	}
	if e := s.raft.Barrier(5 * time.Second).Error(); e != nil {
		return ErrUnavailable
	}
	return nil
}
func (s *Store) apply(c command) error {
	b, e := json.Marshal(c)
	if e != nil {
		return e
	}
	future := s.raft.Apply(b, 5*time.Second)
	if e = future.Error(); e != nil {
		return ErrUnavailable
	}
	if result := future.Response(); result != nil {
		if e, ok := result.(error); ok {
			return e
		}
	}
	return nil
}
func (s *Store) Status() map[string]any {
	s.repairMu.Lock()
	pending, last, replacementError, membershipError := s.pending, s.repairError, s.replacementError, s.membershipError
	s.repairMu.Unlock()
	s.fsm.mu.RLock()
	index := s.fsm.state.Index
	tombstones := 0
	for _, r := range s.fsm.state.Records {
		if r.Deleted {
			tombstones++
		}
	}
	s.fsm.mu.RUnlock()
	return map[string]any{"mode": "consensus", "term": s.raft.Stats()["term"], "state": s.raft.State().String(), "leader": s.LeaderURL(), "applied_index": index, "repair_pending": pending, "repair_error": last, "tombstones": tombstones, "raft_id": string(s.localID), "voters": s.voterCount(), "quorum": s.requiredQuorum(), "members": s.members(), "replacement": s.replacement(), "replacement_error": replacementError, "membership_change": s.membershipChange(), "membership_error": membershipError, "target_rf": s.TargetRF()}
}
