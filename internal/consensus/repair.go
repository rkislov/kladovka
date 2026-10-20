// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rkislov/kladovka/internal/storage"
)

func (s *Store) obtain(hash string) ([]byte, error) {
	if b, e := s.fsm.chunk(hash); e == nil {
		return b, nil
	}
	// Uncompacted committed WAL is also a recovery source, including after restart.
	s.fsm.mu.RLock()
	index, known := s.fsm.state.ChunkLogs[hash]
	s.fsm.mu.RUnlock()
	if !known {
		return nil, storage.ErrIntegrity
	}
	var entry raft.Log
	if s.logs.GetLog(index, &entry) == nil {
		var c command
		if json.Unmarshal(entry.Data, &c) == nil && c.Kind == "chunk" && c.Hash == hash {
			if e := s.Store.CheckFreeSpace(3 * ChunkSize); e != nil {
				return nil, e
			}
			if e := s.fsm.save(hash, c.Data); e == nil {
				return c.Data, nil
			}
		}
	}
	for _, node := range s.PeerNodes() {
		if node.NodeID == s.cfg.NodeID {
			continue
		}
		ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
		req, e := http.NewRequestWithContext(ctx, "GET", node.URL+"/internal/consensus/chunk/"+hash, nil)
		if e != nil {
			cancel()
			continue
		}
		s.reg.Authorize(req)
		resp, e := s.reg.Client().Do(req)
		if e != nil {
			cancel()
			continue
		}
		b, e := io.ReadAll(io.LimitReader(resp.Body, ChunkSize+1))
		resp.Body.Close()
		cancel()
		if resp.StatusCode == 200 && e == nil {
			if e = s.Store.CheckFreeSpace(3 * ChunkSize); e != nil {
				return nil, e
			}
			if e = s.fsm.save(hash, b); e == nil {
				return b, nil
			}
		}
	}
	return nil, ErrUnavailable
}
func (s *Store) ChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}
	hash := r.PathValue("hash")
	if !validHash(hash) {
		http.Error(w, "invalid chunk", 400)
		return
	}
	// Only local verified bytes, never recursive peer fetching.
	b, e := s.fsm.chunk(hash)
	if e != nil {
		http.Error(w, "chunk unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(b)
}
func (s *Store) repairLoop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	chunkCursor, recordCursor := 0, 0
	lastPrune := time.Now()
	retryAt := map[string]time.Time{}
	backoff := map[string]time.Duration{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.SyncTopology()
		s.reconcileMaintenance()
		s.reconcileMembership()
		s.reconcileReplacement()
		if time.Since(lastPrune) >= time.Minute && s.IsLeader() && s.writeMu.TryLock() {
			if s.Barrier() == nil {
				_ = s.apply(command{Kind: "prune"})
			}
			s.writeMu.Unlock()
			lastPrune = time.Now()
		}
		state := s.fsm.copyState()
		hashes := make([]string, 0, len(state.Chunks))
		for h := range state.Chunks {
			hashes = append(hashes, h)
		}
		sort.Strings(hashes)
		last := ""
		for i := 0; i < 8 && i < len(hashes); i++ {
			h := hashes[chunkCursor%len(hashes)]
			chunkCursor++
			if time.Now().Before(retryAt[h]) {
				continue
			}
			if _, e := s.obtain(h); e != nil {
				last = e.Error()
				d := backoff[h] * 2
				if d < time.Second {
					d = time.Second
				}
				if d > time.Minute {
					d = time.Minute
				}
				backoff[h] = d
				retryAt[h] = time.Now().Add(d)
			} else {
				delete(backoff, h)
				delete(retryAt, h)
			}
		}
		keys := make([]string, 0, len(state.Records))
		for k := range state.Records {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > 0 {
			key := keys[recordCursor%len(keys)]
			recordCursor++
			r := state.Records[key]
			s.cacheMu.Lock()
			// Re-check generation under the cache lock; only this worker materializes.
			s.fsm.mu.RLock()
			latest := s.fsm.state.Records[key]
			s.fsm.mu.RUnlock()
			var e error
			if latest.Index == r.Index {
				if r.Deleted {
					e = s.Store.DeleteObject(r.Polka, r.Key)
					if errors.Is(e, storage.ErrObjectNotFound) {
						e = nil
					}
				} else {
					e = s.Store.CreatePolka(r.Polka)
					if errors.Is(e, storage.ErrPolkaExists) {
						e = nil
					}
					if e == nil {
						meta, checkErr := s.Store.HeadObject(r.Polka, r.Key)
						if checkErr != nil || meta.SHA256 != r.Meta.SHA256 {
							reader := &chunkReader{s: s, chunks: r.Chunks, size: r.Meta.Size, cached: -1}
							_, e = s.Store.PutObjectWithETag(r.Polka, r.Key, reader, r.Meta.ContentType, r.Meta.ETag)
						}
					}
				}
				if e == nil {
					s.repairIndex[key] = r.Index
				} else {
					last = e.Error()
				}
			}
			s.cacheMu.Unlock()
		}
		// The cache never serves authoritative reads. Old files are harmless but clean
		// up empty dropped shelves once their tombstones have been materialized.
		names, _ := s.Store.ListPolki()
		for _, name := range names {
			if !state.Buckets[name] {
				_ = s.Store.DeletePolka(name)
			}
		}
		pending := len(retryAt)
		for key, r := range state.Records {
			if s.repairIndex[key] != r.Index {
				pending++
			}
		}
		s.repairMu.Lock()
		s.pending = pending
		s.repairError = last
		s.repairMu.Unlock()
	}
}
