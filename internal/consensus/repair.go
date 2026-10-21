// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"container/list"
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

// repairQueue is a deduplicated FIFO. New keys and generations do not move
// existing work backwards; a failed key yields its turn to healthy records.
// Only the repair worker owns this structure.
type repairQueue struct {
	order   list.List
	entries map[string]*list.Element
}
type repairWork struct {
	key   string
	index uint64
	retry time.Time
	delay time.Duration
	err   string
}

func (q *repairQueue) add(key string, index uint64) {
	if q.entries == nil {
		q.entries = map[string]*list.Element{}
	}
	if el := q.entries[key]; el != nil {
		work := el.Value.(*repairWork)
		if work.index != index {
			*work = repairWork{key: key, index: index}
		}
		return
	}
	q.entries[key] = q.order.PushBack(&repairWork{key: key, index: index})
}
func (q *repairQueue) remove(key string) {
	if el := q.entries[key]; el != nil {
		q.order.Remove(el)
		delete(q.entries, key)
	}
}
func (w *repairWork) failed(err error, now time.Time) {
	w.err = err.Error()
	w.delay *= 2
	if w.delay < time.Second {
		w.delay = time.Second
	}
	if w.delay > time.Minute {
		w.delay = time.Minute
	}
	w.retry = now.Add(w.delay)
}

// Key cursors survive insertions before the current position, unlike offsets.
func nextRepairKey(keys []string, after string) string {
	if len(keys) == 0 {
		return ""
	}
	i := sort.Search(len(keys), func(i int) bool { return keys[i] > after })
	return keys[i%len(keys)]
}

func (s *Store) materialize(key string, r Record) error {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.fsm.mu.RLock()
	latest, exists := s.fsm.state.Records[key]
	s.fsm.mu.RUnlock()
	if !exists || latest.Index != r.Index {
		return nil
	}
	var err error
	if r.Deleted {
		err = s.Store.DeleteObject(r.Polka, r.Key)
		if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, storage.ErrPolkaNotFound) {
			err = nil
		}
	} else {
		err = s.Store.CreatePolka(r.Polka)
		if errors.Is(err, storage.ErrPolkaExists) {
			err = nil
		}
		if err == nil {
			meta, checkErr := s.Store.HeadObject(r.Polka, r.Key)
			if checkErr != nil || meta.SHA256 != r.Meta.SHA256 || meta.Size != r.Meta.Size || meta.ETag != r.Meta.ETag || meta.ContentType != r.Meta.ContentType {
				reader := &chunkReader{s: s, chunks: r.Chunks, size: r.Meta.Size, cached: -1}
				_, err = s.Store.PutObjectWithETag(r.Polka, r.Key, reader, r.Meta.ContentType, r.Meta.ETag)
			}
		}
	}
	s.repairMu.Lock()
	// A newer command can commit while IO runs. Never acknowledge that generation.
	s.fsm.mu.RLock()
	latest, exists = s.fsm.state.Records[key]
	if err == nil && exists && latest.Index == r.Index {
		s.repairIndex[key] = r.Index
	} else {
		delete(s.repairIndex, key)
	}
	s.fsm.mu.RUnlock()
	s.repairMu.Unlock()
	return err
}

// Each turn starts at most 32 operations, with a cooperative 50ms budget.
// A single durable write/network fetch can exceed the budget; it is not aborted
// mid-transaction. There is one worker, keeping IO and memory bounded.
func (s *Store) materializeBatch(ctx context.Context, q *repairQueue) {
	deadline := time.Now().Add(50 * time.Millisecond)
	count := q.order.Len()
	if count > 32 {
		count = 32
	}
	for i := 0; i < count && time.Now().Before(deadline); i++ {
		if ctx.Err() != nil {
			return
		}
		el := q.order.Front()
		w := el.Value.(*repairWork)
		q.order.MoveToBack(el)
		s.fsm.mu.RLock()
		r, exists := s.fsm.state.Records[w.key]
		s.fsm.mu.RUnlock()
		if !exists {
			q.remove(w.key)
			continue
		}
		if r.Index != w.index {
			*w = repairWork{key: w.key, index: r.Index}
		}
		if time.Now().Before(w.retry) {
			continue
		}
		if err := s.materialize(w.key, r); err != nil {
			w.failed(err, time.Now())
		} else {
			q.remove(w.key)
		}
	}
}

func (s *Store) refreshRepairQueue(q *repairQueue, scrubAfter *string) ([]string, map[string]bool) {
	// No JSON clone of the full catalogue. Copy only scheduling keys/generations.
	s.repairMu.Lock()
	s.fsm.mu.RLock()
	keys := make([]string, 0, len(s.fsm.state.Records))
	for key, r := range s.fsm.state.Records {
		keys = append(keys, key)
		if s.repairIndex[key] != r.Index {
			q.add(key, r.Index)
		}
	}
	for key := range q.entries {
		if _, ok := s.fsm.state.Records[key]; !ok {
			q.remove(key)
		}
	}
	for key := range s.repairIndex {
		if _, ok := s.fsm.state.Records[key]; !ok {
			delete(s.repairIndex, key)
		}
	}
	sort.Strings(keys)
	if key := nextRepairKey(keys, *scrubAfter); key != "" {
		*scrubAfter = key
		q.add(key, s.fsm.state.Records[key].Index)
	}
	hashes := make([]string, 0, len(s.fsm.state.Chunks))
	for h := range s.fsm.state.Chunks {
		hashes = append(hashes, h)
	}
	buckets := make(map[string]bool, len(s.fsm.state.Buckets))
	for name, v := range s.fsm.state.Buckets {
		buckets[name] = v
	}
	s.fsm.mu.RUnlock()
	s.repairMu.Unlock()
	sort.Strings(hashes)
	return hashes, buckets
}

func (s *Store) repairLoop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var q repairQueue
	var chunkCursor, recordCursor string
	var refreshed time.Time
	lastPrune := time.Now()
	chunkRetries := map[string]*repairWork{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if time.Since(refreshed) >= time.Second {
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
			hashes, buckets := s.refreshRepairQueue(&q, &recordCursor)
			live := make(map[string]bool, len(hashes))
			for _, h := range hashes {
				live[h] = true
			}
			for h := range chunkRetries {
				if !live[h] {
					delete(chunkRetries, h)
				}
			}
			for i := 0; i < 8 && i < len(hashes); i++ {
				if ctx.Err() != nil {
					return
				}
				h := nextRepairKey(hashes, chunkCursor)
				chunkCursor = h
				w := chunkRetries[h]
				if w != nil && time.Now().Before(w.retry) {
					continue
				}
				if _, err := s.obtain(h); err != nil {
					if w == nil {
						w = &repairWork{}
						chunkRetries[h] = w
					}
					w.failed(err, time.Now())
				} else {
					delete(chunkRetries, h)
				}
			}
			// The cache never serves authoritative reads. Drop only empty shelves.
			names, _ := s.Store.ListPolki()
			for _, name := range names {
				if !buckets[name] {
					_ = s.Store.DeletePolka(name)
				}
			}
			refreshed = time.Now()
		}
		s.materializeBatch(ctx, &q)
		last := ""
		for _, w := range chunkRetries {
			last = w.err
			break
		}
		if last == "" {
			for _, el := range q.entries {
				if err := el.Value.(*repairWork).err; err != "" {
					last = err
					break
				}
			}
		}
		s.repairMu.Lock()
		s.repairChunkPending = len(chunkRetries) // record generations are counted live by Status
		s.repairError = last
		s.repairMu.Unlock()
	}
}
