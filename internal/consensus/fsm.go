// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/rkislov/kladovka/internal/storage"
)

const ChunkSize = 256 << 10

var ErrUnavailable = errors.New("consensus quorum or committed data unavailable")

type Chunk struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}
type Record struct {
	Polka   string             `json:"polka"`
	Key     string             `json:"key"`
	Meta    storage.ObjectMeta `json:"meta"`
	Chunks  []Chunk            `json:"chunks"`
	Deleted bool               `json:"deleted"`
	Index   uint64             `json:"index"`
}
type command struct {
	Kind        string       `json:"kind"`
	Polka       string       `json:"polka,omitempty"`
	Key         string       `json:"key,omitempty"`
	Record      Record       `json:"record,omitempty"`
	Hash        string       `json:"hash,omitempty"`
	Data        []byte       `json:"data,omitempty"`
	Replacement *Replacement `json:"replacement,omitempty"`
}
type state struct {
	Buckets          map[string]bool   `json:"buckets"`
	Records          map[string]Record `json:"records"`
	Chunks           map[string]int64  `json:"chunks"`
	ChunkLogs        map[string]uint64 `json:"chunk_logs"`
	Index            uint64            `json:"index"`
	Replacement      *Replacement      `json:"replacement,omitempty"`
	Retired          []string          `json:"retired,omitempty"`
	RetiredAddresses []string          `json:"retired_addresses,omitempty"`
}
type machine struct {
	mu           sync.RWMutex
	state        state
	dir          string
	diskError    error
	snapshotRefs map[string]int
	free         func(uint64) error
}

func emptyState() state {
	return state{Buckets: map[string]bool{}, Records: map[string]Record{}, Chunks: map[string]int64{}, ChunkLogs: map[string]uint64{}}
}
func validHash(h string) bool {
	b, e := hex.DecodeString(h)
	return e == nil && len(b) == 32 && h == hex.EncodeToString(b)
}
func (m *machine) path(hash string) string { return filepath.Join(m.dir, hash) }
func (m *machine) save(hash string, data []byte) error {
	sum := sha256.Sum256(data)
	if !validHash(hash) || hex.EncodeToString(sum[:]) != hash || len(data) > ChunkSize {
		return storage.ErrIntegrity
	}
	if m.free != nil {
		if e := m.free(uint64(len(data))); e != nil {
			return e
		}
	}
	f, e := os.CreateTemp(m.dir, "chunk-*.tmp")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(data); e == nil {
		e = f.Sync()
	}
	if closeErr := f.Close(); e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), m.path(hash)); e != nil {
		return e
	}
	return syncDirectory(m.dir)
}
func syncDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func (m *machine) chunk(hash string) ([]byte, error) {
	if !validHash(hash) {
		return nil, storage.ErrIntegrity
	}
	file, e := os.Open(m.path(hash))
	if e != nil {
		return nil, e
	}
	defer file.Close()
	b, e := io.ReadAll(io.LimitReader(file, ChunkSize+1))
	if e != nil {
		return nil, e
	}
	sum := sha256.Sum256(b)
	if len(b) > ChunkSize || hex.EncodeToString(sum[:]) != hash {
		return nil, storage.ErrIntegrity
	}
	return b, nil
}
func (m *machine) Apply(l *raft.Log) interface{} {
	var c command
	if json.Unmarshal(l.Data, &c) != nil {
		return errors.New("invalid consensus command")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Index = l.Index
	switch c.Kind {
	case "replace":
		if c.Replacement == nil {
			return errors.New("missing replacement")
		}
		r := *c.Replacement
		if m.state.Replacement != nil && !m.state.Replacement.Done {
			return errors.New("replacement already pending")
		}
		for _, id := range m.state.Retired {
			if r.NewID == id {
				return errors.New("retired incarnation")
			}
		}
		m.state.Replacement = &r
		return nil
	case "replace_done":
		if c.Replacement == nil || m.state.Replacement == nil || m.state.Replacement.NewID != c.Replacement.NewID {
			return errors.New("replacement differs")
		}
		if !m.state.Replacement.Done {
			m.state.Retired = append(m.state.Retired, m.state.Replacement.OldID)
			m.state.RetiredAddresses = append(m.state.RetiredAddresses, m.state.Replacement.OldAddress)
		}
		m.state.Replacement.Done = true
		return nil

	case "chunk":
		if !validHash(c.Hash) || len(c.Data) > ChunkSize {
			return storage.ErrIntegrity
		}
		sum := sha256.Sum256(c.Data)
		if hex.EncodeToString(sum[:]) != c.Hash {
			return storage.ErrIntegrity
		}
		// Logical state always advances identically. Failed local IO is repair work,
		// not a divergent vote on an already committed operation.
		m.state.Chunks[c.Hash] = int64(len(c.Data))
		m.state.ChunkLogs[c.Hash] = l.Index
		if e := m.save(c.Hash, c.Data); e != nil {
			m.diskError = e
			return e
		}
		return nil
	case "create":
		if m.state.Buckets[c.Polka] {
			return storage.ErrPolkaExists
		}
		m.state.Buckets[c.Polka] = true
		return nil
	case "drop":
		if !m.state.Buckets[c.Polka] {
			return storage.ErrPolkaNotFound
		}
		for _, r := range m.state.Records {
			if r.Polka == c.Polka && !r.Deleted {
				return storage.ErrPolkaNotEmpty
			}
		}
		delete(m.state.Buckets, c.Polka)
		return nil
	case "put":
		r := c.Record
		if !m.state.Buckets[r.Polka] {
			return storage.ErrPolkaNotFound
		}
		var size int64
		for _, p := range r.Chunks {
			n, ok := m.state.Chunks[p.Hash]
			if !ok || n != p.Size {
				return storage.ErrIntegrity
			}
			size += n
		}
		if size != r.Meta.Size || r.Key != r.Meta.Key || !validHash(r.Meta.SHA256) {
			return storage.ErrIntegrity
		}
		r.Index = l.Index
		r.Deleted = false
		m.state.Records[r.Polka+"/"+r.Key] = r
		return nil
	case "prune":
		live := map[string]bool{}
		for _, r := range m.state.Records {
			if !r.Deleted && m.state.Buckets[r.Polka] {
				for _, c := range r.Chunks {
					live[c.Hash] = true
				}
			}
		}
		for hash := range m.state.Chunks {
			if !live[hash] {
				delete(m.state.Chunks, hash)
				delete(m.state.ChunkLogs, hash)
				if m.snapshotRefs[hash] == 0 {
					_ = os.Remove(m.path(hash))
				}
			}
		}
		return syncDirectory(m.dir)
	case "delete":
		if !m.state.Buckets[c.Polka] {
			return storage.ErrPolkaNotFound
		}
		// Never discard deletion generations: an old file cannot become live again.
		m.state.Records[c.Polka+"/"+c.Key] = Record{Polka: c.Polka, Key: c.Key, Deleted: true, Index: l.Index}
		return nil
	default:
		return errors.New("unknown consensus command")
	}
}
func (m *machine) copyState() state {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, _ := json.Marshal(m.state)
	var s state
	_ = json.Unmarshal(b, &s)
	return s
}

type snapshot struct {
	s state
	m *machine
}

func (m *machine) Snapshot() (raft.FSMSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	body, e := json.Marshal(m.state)
	if e != nil {
		return nil, e
	}
	if len(body) > 64<<20 {
		return nil, errors.New("consensus snapshot catalog exceeds restore limit")
	}
	var st state
	if e = json.Unmarshal(body, &st); e != nil {
		return nil, e
	}
	if m.snapshotRefs == nil {
		m.snapshotRefs = map[string]int{}
	}
	for hash := range st.Chunks {
		m.snapshotRefs[hash]++
	}
	return &snapshot{s: st, m: m}, nil
}
func (m *machine) releaseChunks(chunks []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, hash := range chunks {
		m.snapshotRefs[hash]--
		if m.snapshotRefs[hash] == 0 {
			delete(m.snapshotRefs, hash)
			if _, needed := m.state.Chunks[hash]; !needed {
				_ = os.Remove(m.path(hash))
			}
		}
	}
	_ = syncDirectory(m.dir)
}
func (s *snapshot) Release() {
	hashes := make([]string, 0, len(s.s.Chunks))
	for hash := range s.s.Chunks {
		hashes = append(hashes, hash)
	}
	s.m.releaseChunks(hashes)
}
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	tw := tar.NewWriter(sink)
	fail := func(e error) error { _ = sink.Cancel(); return e }
	b, e := json.Marshal(s.s)
	if e != nil {
		return fail(e)
	}
	if e = tw.WriteHeader(&tar.Header{Name: "state.json", Mode: 0600, Size: int64(len(b))}); e != nil {
		return fail(e)
	}
	if _, e = tw.Write(b); e != nil {
		return fail(e)
	}
	for hash, size := range s.s.Chunks {
		b, e := s.m.chunk(hash)
		if e != nil || int64(len(b)) != size {
			if e == nil {
				e = storage.ErrIntegrity
			}
			return fail(e)
		}
		if e = tw.WriteHeader(&tar.Header{Name: hash, Mode: 0600, Size: size}); e != nil {
			return fail(e)
		}
		if _, e = tw.Write(b); e != nil {
			return fail(e)
		}
	}
	if e = tw.Close(); e != nil {
		return fail(e)
	}
	return sink.Close()
}
func (m *machine) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	tr := tar.NewReader(reader)
	h, e := tr.Next()
	if e != nil || h.Name != "state.json" || h.Size > 64<<20 {
		return errors.New("invalid consensus snapshot")
	}
	var s state
	if e = json.NewDecoder(tr).Decode(&s); e != nil {
		return e
	}
	if s.Buckets == nil || s.Records == nil || s.Chunks == nil || s.ChunkLogs == nil {
		return errors.New("incomplete consensus snapshot")
	}
	found := map[string]bool{}
	for {
		h, e = tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		_, exists := s.Chunks[h.Name]
		if !exists || !validHash(h.Name) || h.Size > ChunkSize || h.Size < 0 || s.Chunks[h.Name] != h.Size || found[h.Name] {
			return errors.New("invalid snapshot chunk")
		}
		b, e := io.ReadAll(tr)
		if e != nil {
			return e
		}
		if e = m.save(h.Name, b); e != nil {
			return fmt.Errorf("restore chunk: %w", e)
		}
		found[h.Name] = true
	}
	if len(found) != len(s.Chunks) {
		return errors.New("snapshot missing chunks")
	}
	m.mu.Lock()
	m.state = s
	m.diskError = nil
	m.mu.Unlock()
	return nil
}
