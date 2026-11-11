// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/rkislov/kladovka/internal/storage"
)

func (m *machine) keepVersion(r Record) {
	if m.state.Versions == nil {
		m.state.Versions = map[string][]Record{}
	}
	key := r.Polka + "/" + r.Key
	if len(m.state.Versions[key]) == 0 {
		if old, ok := m.state.Records[key]; ok && !old.Deleted {
			if old.Meta.VersionID == "" {
				old.Meta.VersionID = "null"
			}
			m.state.Versions[key] = []Record{old}
		}
	}
	m.state.Versions[key] = append(m.state.Versions[key], r)
}
func (m *machine) purgeVersion(c command, index uint64) error {
	key := c.Polka + "/" + c.Key
	versions := m.state.Versions[key]
	if len(versions) == 0 && c.VersionID == "null" {
		if current, ok := m.state.Records[key]; ok && !current.Deleted && current.Meta.VersionID == "" {
			m.state.Records[key] = Record{Polka: c.Polka, Key: c.Key, Deleted: true, Index: index}
			return nil
		}
	}
	found := false
	out := make([]Record, 0, len(versions))
	for _, r := range versions {
		if r.Meta.VersionID == c.VersionID {
			found = true
		} else {
			out = append(out, r)
		}
	}
	if !found {
		return storage.ErrObjectNotFound
	}
	m.state.Versions[key] = out
	current := m.state.Records[key]
	if current.Meta.VersionID == c.VersionID {
		if len(out) > 0 {
			current = out[len(out)-1]
		} else {
			current = Record{Polka: c.Polka, Key: c.Key, Deleted: true}
		}
		current.Index = index
		m.state.Records[key] = current
	}
	if len(out) == 0 {
		delete(m.state.Versions, key)
	}
	return nil
}
func (m *machine) expireTrash(c command, index uint64) error {
	key := c.Polka + "/" + c.Key
	r := m.state.Records[key]
	// A stale sweep must never remove a restored or newly deleted generation.
	if !r.Deleted || r.Meta.VersionID != c.VersionID {
		return nil
	}
	delete(m.state.Versions, key)
	m.state.Records[key] = Record{Polka: c.Polka, Key: c.Key, Deleted: true, Index: index}
	return nil
}
func (s *Store) deleteVersionedLocked(polka, key string) (*storage.ObjectMeta, error) {
	meta := storage.ObjectMeta{Key: key, DeleteMarker: true, LastModified: time.Now().UTC()}
	if s.cfg.VersioningEnabled {
		meta.VersionID = storage.NewVersionID()
	}
	err := s.apply(command{Kind: "delete", Polka: polka, Key: key, Versioned: s.cfg.VersioningEnabled, Record: Record{Meta: meta}})
	return &meta, err
}
func (s *Store) TrashObject(polka, key string) (*storage.ObjectMeta, error) {
	if !validShelf(polka) || !validKey(key) {
		return nil, storage.ErrObjectNotFound
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.pruneIfDue()
	return s.deleteVersionedLocked(polka, key)
}
func (s *Store) ListVersions(polka, prefix string) ([]storage.ObjectMeta, error) {
	if err := s.Barrier(); err != nil {
		return nil, err
	}
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if !s.fsm.state.Buckets[polka] {
		return nil, storage.ErrPolkaNotFound
	}
	out := []storage.ObjectMeta{}
	for key, versions := range s.fsm.state.Versions {
		for i := len(versions) - 1; i >= 0; i-- {
			r := versions[i]
			if r.Polka == polka && strings.HasPrefix(r.Key, prefix) {
				meta := r.Meta
				meta.IsLatest = s.fsm.state.Records[key].Meta.VersionID == meta.VersionID
				out = append(out, meta)
			}
		}
	}
	// Existing unversioned objects remain readable through the null version.
	for key, r := range s.fsm.state.Records {
		if len(s.fsm.state.Versions[key]) == 0 && !r.Deleted && r.Polka == polka && strings.HasPrefix(r.Key, prefix) {
			meta := r.Meta
			meta.VersionID = "null"
			meta.IsLatest = true
			out = append(out, meta)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
func (s *Store) GetVersion(polka, key, id string) (io.ReadCloser, *storage.ObjectMeta, error) {
	if err := s.Barrier(); err != nil {
		return nil, nil, err
	}
	s.fsm.mu.Lock()
	var r Record
	found := false
	for _, v := range s.fsm.state.Versions[polka+"/"+key] {
		if v.Meta.VersionID == id {
			r = v
			found = true
			break
		}
	}
	if !found && id == "null" {
		v, ok := s.fsm.state.Records[polka+"/"+key]
		if ok && v.Meta.VersionID == "" && !v.Deleted {
			r = v
			r.Meta.VersionID = "null"
			found = true
		}
	}
	if !found {
		s.fsm.mu.Unlock()
		return nil, nil, storage.ErrObjectNotFound
	}
	if r.Deleted {
		s.fsm.mu.Unlock()
		return nil, &r.Meta, storage.ErrObjectNotFound
	}
	if s.fsm.snapshotRefs == nil {
		s.fsm.snapshotRefs = map[string]int{}
	}
	hashes := []string{}
	for _, c := range r.Chunks {
		s.fsm.snapshotRefs[c.Hash]++
		hashes = append(hashes, c.Hash)
	}
	s.fsm.mu.Unlock()
	reader := &chunkReader{s: s, chunks: r.Chunks, size: r.Meta.Size, cached: -1, release: func() { s.fsm.releaseChunks(hashes) }}
	hash := sha256.New()
	n, err := io.Copy(hash, reader)
	if err != nil {
		reader.Close()
		return nil, nil, err
	}
	if n != r.Meta.Size || hex.EncodeToString(hash.Sum(nil)) != r.Meta.SHA256 {
		reader.Close()
		return nil, nil, storage.ErrIntegrity
	}
	reader.Seek(0, io.SeekStart)
	return reader, &r.Meta, nil
}
func (s *Store) DeleteVersion(polka, key, id string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.apply(command{Kind: "purge_version", Polka: polka, Key: key, VersionID: id})
}
func (s *Store) ExpireTrash(polka, key, id string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.apply(command{Kind: "expire_trash", Polka: polka, Key: key, VersionID: id})
}
