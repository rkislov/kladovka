// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type historyIndex struct {
	Polka    string       `json:"polka"`
	Key      string       `json:"key"`
	Versions []ObjectMeta `json:"versions"`
}

func (s *Store) historyDir(polka, key string) string {
	h := sha256.Sum256([]byte(polka + "/" + key))
	return filepath.Join(s.root, ".versions", hex.EncodeToString(h[:]))
}
func (s *Store) readHistory(polka, key string) (historyIndex, error) {
	v := historyIndex{Polka: polka, Key: key}
	b, err := os.ReadFile(filepath.Join(s.historyDir(polka, key), "index.json"))
	if errors.Is(err, os.ErrNotExist) {
		return v, nil
	}
	if err == nil {
		err = json.Unmarshal(b, &v)
	}
	return v, err
}
func validVersion(id string) bool {
	if id == "null" {
		return true
	}
	b, e := hex.DecodeString(id)
	return e == nil && len(b) == 16 && hex.EncodeToString(b) == id
}
func (s *Store) commitHistory(txdir, op string, tx transaction) error {
	dir := s.historyDir(tx.Polka, tx.Key)
	if err := durableMkdir(dir, s.root); err != nil {
		return err
	}
	h, err := s.readHistory(tx.Polka, tx.Key)
	if err != nil {
		return err
	}
	if tx.History != nil {
		h.Versions = *tx.History
	} else {
		if tx.Meta == nil || !validVersion(tx.Meta.VersionID) {
			return ErrIntegrity
		}
		for _, v := range h.Versions {
			if v.VersionID == tx.Meta.VersionID {
				return nil
			}
		}
		if len(h.Versions) == 0 {
			old, e := s.readSideMeta(op)
			if e == nil {
				if old.VersionID == "" {
					old.VersionID = "null"
				}
				if old.VersionID != tx.Meta.VersionID {
					if err = os.Link(op, filepath.Join(dir, old.VersionID+".data")); err != nil && !errors.Is(err, os.ErrExist) {
						return err
					}
					h.Versions = append(h.Versions, *old)
				}
			} else if !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
		if !tx.Delete {
			dest := filepath.Join(dir, tx.Meta.VersionID+".data")
			if err = os.Link(filepath.Join(txdir, "data"), dest); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			if err = verifyFile(dest, tx.Meta.SHA256); err != nil {
				return err
			}
		}
		h.Versions = append(h.Versions, *tx.Meta)
	}
	b, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, "index.json"), b)
}
func (s *Store) ListVersions(polka, prefix string) ([]ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ok, _ := s.PolkaExists(polka); !ok {
		return nil, ErrPolkaNotFound
	}
	out := []ObjectMeta{}
	seen := map[string]bool{}
	entries, err := os.ReadDir(filepath.Join(s.root, ".versions"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, d := range entries {
		b, e := os.ReadFile(filepath.Join(s.root, ".versions", d.Name(), "index.json"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return nil, e
		}
		var h historyIndex
		if e = json.Unmarshal(b, &h); e != nil {
			return nil, e
		}
		if h.Polka != polka || !strings.HasPrefix(h.Key, prefix) {
			continue
		}
		if s.pending[polka+"/"+h.Key] {
			return nil, ErrPendingWrite
		}
		seen[h.Key] = true
		for i := len(h.Versions) - 1; i >= 0; i-- {
			v := h.Versions[i]
			v.IsLatest = i == len(h.Versions)-1
			out = append(out, v)
		}
	}
	// Old files are null versions until their first versioned mutation.
	pp, _ := s.polkaPath(polka)
	err = filepath.WalkDir(pp, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() || !strings.HasSuffix(path, ".kladovka-meta.json") {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		var v ObjectMeta
		if e = json.Unmarshal(b, &v); e != nil {
			return e
		}
		if !seen[v.Key] && strings.HasPrefix(v.Key, prefix) {
			v.VersionID = "null"
			v.IsLatest = true
			out = append(out, v)
		}
		return nil
	})
	sort.SliceStable(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, err
}
func (s *Store) GetVersion(polka, key, id string) (io.ReadCloser, *ObjectMeta, error) {
	if !validVersion(id) {
		return nil, nil, ErrObjectNotFound
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.pending[polka+"/"+key] {
		return nil, nil, ErrPendingWrite
	}
	h, err := s.readHistory(polka, key)
	if err != nil {
		return nil, nil, err
	}
	for _, v := range h.Versions {
		if v.VersionID == id {
			if v.DeleteMarker {
				return nil, &v, ErrObjectNotFound
			}
			path := filepath.Join(s.historyDir(polka, key), id+".data")
			if err = verifyFile(path, v.SHA256); err != nil {
				return nil, nil, err
			}
			f, e := os.Open(path)
			return f, &v, e
		}
	}
	if id == "null" && len(h.Versions) == 0 {
		op, e := s.objectPath(polka, key)
		if e != nil {
			return nil, nil, e
		}
		v, e := s.readSideMeta(op)
		if e != nil {
			return nil, nil, ErrObjectNotFound
		}
		if e = verifyFile(op, v.SHA256); e != nil {
			return nil, nil, e
		}
		f, e := os.Open(op)
		v.VersionID = "null"
		return f, v, e
	}
	return nil, nil, ErrObjectNotFound
}
func (s *Store) rewriteHistory(polka, key string, versions []ObjectMeta) error {
	dir, err := os.MkdirTemp(s.transactionRoot(), "history-")
	if err != nil {
		return err
	}
	tx := transaction{Polka: polka, Key: key, History: &versions, Delete: true}
	if len(versions) > 0 {
		v := versions[len(versions)-1]
		tx.Meta = &v
		tx.Delete = v.DeleteMarker
		if !tx.Delete {
			if err = os.Link(filepath.Join(s.historyDir(polka, key), v.VersionID+".data"), filepath.Join(dir, "data")); err != nil {
				os.RemoveAll(dir)
				return err
			}
		}
	}
	b, _ := json.Marshal(tx)
	if err = writeAtomic(filepath.Join(dir, "manifest.json"), b); err != nil {
		return err
	}
	if err = syncDir(s.transactionRoot()); err != nil {
		return err
	}
	if err = s.commitTransaction(dir, tx, false); err != nil {
		s.pending[polka+"/"+key] = true
	}
	return err
}
func (s *Store) DeleteVersion(polka, key, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.recoverPending(); err != nil {
		return err
	}
	h, err := s.readHistory(polka, key)
	if err != nil {
		return err
	}
	if len(h.Versions) == 0 && id == "null" {
		op, e := s.objectPath(polka, key)
		if e != nil {
			return e
		}
		if _, e = s.readSideMeta(op); e == nil {
			return s.rewriteHistory(polka, key, []ObjectMeta{})
		}
	}
	out := []ObjectMeta{}
	found := false
	for _, v := range h.Versions {
		if v.VersionID == id {
			found = true
		} else {
			out = append(out, v)
		}
	}
	if !found {
		return ErrObjectNotFound
	}
	if err = s.rewriteHistory(polka, key, out); err != nil {
		return err
	}
	if validVersion(id) {
		_ = os.Remove(filepath.Join(s.historyDir(polka, key), id+".data"))
	}
	return nil
}
func (s *Store) ExpireTrash(polka, key, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.recoverPending(); err != nil {
		return err
	}
	h, err := s.readHistory(polka, key)
	if err != nil {
		return err
	}
	if len(h.Versions) == 0 {
		return nil
	}
	last := h.Versions[len(h.Versions)-1]
	if !last.DeleteMarker || last.VersionID != id {
		return nil
	}
	if err = s.rewriteHistory(polka, key, []ObjectMeta{}); err != nil {
		return err
	}
	for _, v := range h.Versions {
		if validVersion(v.VersionID) {
			_ = os.Remove(filepath.Join(s.historyDir(polka, key), v.VersionID+".data"))
		}
	}
	return nil
}
func (s *Store) TrashObject(polka, key string) (*ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.deleteLocked(polka, key); err != nil {
		return nil, err
	}
	h, err := s.readHistory(polka, key)
	if err != nil {
		return nil, err
	}
	if len(h.Versions) == 0 {
		return &ObjectMeta{Key: key}, nil
	}
	v := h.Versions[len(h.Versions)-1]
	return &v, nil
}
func (s *Store) PutReplica(polka, key string, body io.Reader, meta ObjectMeta) (*ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicaMeta = &meta
	defer func() { s.replicaMeta = nil }()
	return s.putLocked(polka, key, body, meta.ContentType, meta.ETag)
}
func (s *Store) DeleteReplica(polka, key string, meta ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicaMeta = &meta
	defer func() { s.replicaMeta = nil }()
	return s.deleteLocked(polka, key)
}
func (s *Store) historyBytes() (int64, error) {
	var used int64
	entries, err := os.ReadDir(filepath.Join(s.root, ".versions"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	for _, d := range entries {
		b, e := os.ReadFile(filepath.Join(s.root, ".versions", d.Name(), "index.json"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return 0, e
		}
		var h historyIndex
		if e = json.Unmarshal(b, &h); e != nil {
			return 0, e
		}
		for i, v := range h.Versions {
			if i != len(h.Versions)-1 && !v.DeleteMarker {
				used += v.Size
			}
		}
	}
	return used, nil
}

// EnsureVersioning prevents accidental return to destructive semantics after rollout.
func EnsureVersioning(root string, enabled bool) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	path := filepath.Join(root, ".kladovka-versioning")
	if _, err := os.Stat(path); err == nil {
		if !enabled {
			return errors.New("versioning has been enabled for this DATA_ROOT; disabling is not supported")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if enabled {
		return writeAtomic(path, []byte("1\n"))
	}
	return nil
}
func (s *Store) hasHistory(polka string) (bool, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, ".versions"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, d := range entries {
		b, e := os.ReadFile(filepath.Join(s.root, ".versions", d.Name(), "index.json"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return false, e
		}
		var h historyIndex
		if e = json.Unmarshal(b, &h); e != nil {
			return false, e
		}
		if h.Polka == polka && len(h.Versions) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// CleanupVersionFiles removes only payloads no longer referenced by a committed
// history index. Readers already holding a file descriptor may finish safely.
func (s *Store) CleanupVersionFiles() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.recoverPending(); err != nil {
		return 0, err
	}
	root := filepath.Join(s.root, ".versions")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		dir := filepath.Join(root, entry.Name())
		body, e := os.ReadFile(filepath.Join(dir, "index.json"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return removed, e
		}
		var h historyIndex
		if e = json.Unmarshal(body, &h); e != nil {
			return removed, e
		}
		live := map[string]bool{}
		for _, v := range h.Versions {
			if !v.DeleteMarker {
				live[v.VersionID+".data"] = true
			}
		}
		files, e := os.ReadDir(dir)
		if e != nil {
			return removed, e
		}
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".data") && !live[file.Name()] {
				if e = os.Remove(filepath.Join(dir, file.Name())); e != nil {
					return removed, e
				}
				removed++
			}
		}
		if e = syncDir(dir); e != nil {
			return removed, e
		}
	}
	return removed, nil
}
