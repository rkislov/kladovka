// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package storage implements local filesystem object storage.
// In Кладовка terminology a "полка" (shelf) is an S3 bucket.
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrPolkaNotFound  = errors.New("полка не найдена")
	ErrPolkaExists    = errors.New("полка уже существует")
	ErrObjectNotFound = errors.New("объект не найден")
	ErrPolkaNotEmpty  = errors.New("полка не пуста")
)

// ObjectMeta describes a stored object (S3 HeadObject fields).
type ObjectMeta struct {
	VersionID    string    `json:"version_id,omitempty"`
	DeleteMarker bool      `json:"delete_marker,omitempty"`
	IsLatest     bool      `json:"is_latest,omitempty"`
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	ContentType  string    `json:"content_type"`
	LastModified time.Time `json:"last_modified"`
	SHA256       string    `json:"sha256,omitempty"`
}

// Store is a filesystem-backed object store. Polka = S3 bucket directory.
type Store struct {
	root                string
	mu                  sync.RWMutex
	multipartMu         sync.Mutex
	options             Options
	active              map[string]int
	pending             map[string]bool
	catalogues          map[string]*catalogue
	catalogueExactNames bool
	replicaMeta         *ObjectMeta
	afterStep           func(string) error
}

// New creates a store under root and ensures the directory exists.
func New(root string) (*Store, error) { return NewWithOptions(root, Options{VerifyReads: true}) }

func (s *Store) polkaPath(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || strings.HasPrefix(name, ".") || strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("некорректное имя полки")
	}
	switch name {
	case "api", "ui", "internal", "healthz", "cluster", "metrics":
		return "", fmt.Errorf("зарезервированное имя полки")
	}
	return filepath.Join(s.root, name), nil
}

func (s *Store) objectPath(polka, key string) (string, error) {
	pp, err := s.polkaPath(polka)
	if err != nil {
		return "", err
	}
	key = strings.TrimPrefix(key, "/")
	if key == "" || strings.HasSuffix(key, ".tmp") || strings.HasSuffix(key, ".kladovka-meta.json") || strings.Contains(key, "..") {
		return "", fmt.Errorf("некорректный ключ объекта")
	}
	// Keep path separators as nested dirs.
	parts := strings.Split(key, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return "", fmt.Errorf("некорректный ключ объекта")
		}
	}
	return filepath.Join(append([]string{pp}, parts...)...), nil
}

// CreatePolka creates a shelf (S3 CreateBucket).
func (s *Store) CreatePolka(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pp, err := s.polkaPath(name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(pp); err == nil {
		return ErrPolkaExists
	}
	if err := durableMkdir(pp, s.root); err != nil {
		delete(s.catalogues, pp)
		return err
	}
	if s.catalogueExactNames {
		s.catalogues[pp] = &catalogue{objects: map[string]ObjectMeta{}}
	} else {
		clear(s.catalogues)
	}
	return nil
}

// DeletePolka removes an empty shelf, including retained generations.
func (s *Store) DeletePolka(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if exists, err := s.hasHistory(name); err != nil {
		return err
	} else if exists {
		return ErrPolkaNotEmpty
	}
	pp, err := s.polkaPath(name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(pp); errors.Is(err, os.ErrNotExist) {
		return ErrPolkaNotFound
	}
	entries, err := os.ReadDir(pp)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return ErrPolkaNotEmpty
	}
	if s.catalogueExactNames {
		delete(s.catalogues, pp)
	} else {
		clear(s.catalogues)
	}
	if err := os.Remove(pp); err != nil {
		return err
	}
	return syncDir(s.root)
}

// PolkaExists reports whether the shelf exists.
func (s *Store) PolkaExists(name string) (bool, error) {
	pp, err := s.polkaPath(name)
	if err != nil {
		return false, err
	}
	st, err := os.Stat(pp)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return st.IsDir(), nil
}

// ListPolki returns all shelf names (S3 ListBuckets).
func (s *Store) ListPolki() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// PutObject writes object body to a shelf.
func (s *Store) PutObject(polka, key string, body io.Reader, contentType string) (*ObjectMeta, error) {
	return s.PutObjectWithETag(polka, key, body, contentType, "")
}
func (s *Store) PutObjectWithETag(polka, key string, body io.Reader, contentType, etag string) (*ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(polka, key, body, contentType, etag)
}
func (s *Store) writeSideMeta(path string, meta *ObjectMeta) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return writeAtomic(path+".kladovka-meta.json", b)
}

func (s *Store) readSideMeta(objectPath string) (*ObjectMeta, error) {
	b, err := os.ReadFile(objectPath + ".kladovka-meta.json")
	if err != nil {
		return nil, err
	}
	var m ObjectMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// GetObject opens an object for reading.
func (s *Store) GetObject(polka, key string) (io.ReadCloser, *ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.pending[polka+"/"+key] {
		return nil, nil, ErrPendingWrite
	}
	op, err := s.objectPath(polka, key)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(op)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (io.ReadCloser, *ObjectMeta, error) { f.Close(); return nil, nil, err }
	st, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if st.IsDir() {
		return fail(ErrObjectNotFound)
	}
	meta, err := s.readSideMeta(op)
	if err != nil || meta == nil || meta.Key != key || meta.Size != st.Size() || len(meta.SHA256) != 64 {
		return fail(fmt.Errorf("%w: missing or invalid metadata", ErrIntegrity))
	}
	if _, err = hex.DecodeString(meta.SHA256); err != nil {
		return fail(ErrIntegrity)
	}
	if s.options.VerifyReads {
		hash := sha256.New()
		if _, err = io.Copy(hash, f); err != nil {
			return fail(err)
		}
		if hex.EncodeToString(hash.Sum(nil)) != meta.SHA256 {
			return fail(ErrIntegrity)
		}
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return fail(err)
		}
	}
	meta.Size = st.Size()
	meta.Key = key
	return f, meta, nil
}

// HeadObject returns metadata without body.
func (s *Store) HeadObject(polka, key string) (*ObjectMeta, error) {
	rc, meta, err := s.GetObject(polka, key)
	if err != nil {
		return nil, err
	}
	_ = rc.Close()
	return meta, nil
}

// DeleteObject removes an object.
func (s *Store) DeleteObject(polka, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(polka, key)
}

// ListObjects lists objects under a shelf with optional prefix (flat keys).
func (s *Store) ListObjects(polka, prefix string, maxKeys int, startAfter string) ([]ObjectMeta, bool, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	page, err := s.ListObjectPage(polka, prefix, "", startAfter, maxKeys)
	return page.Objects, page.Truncated, err
}

// PolkaStats is a summary of one shelf for the dashboard.
type PolkaStats struct {
	Name    string `json:"name"`
	Objects int    `json:"objects"`
	Bytes   int64  `json:"bytes"`
}

// StatsForPolka counts objects and bytes on a shelf (skips meta/tmp files).
func (s *Store) StatsForPolka(polka string) (PolkaStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := PolkaStats{Name: polka}
	ok, err := s.PolkaExists(polka)
	if err != nil {
		return st, err
	}
	if !ok {
		return st, ErrPolkaNotFound
	}
	pp, err := s.polkaPath(polka)
	if err != nil {
		return st, err
	}
	err = filepath.WalkDir(pp, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if strings.HasSuffix(d.Name(), ".kladovka-meta.json") || strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return nil
		}
		st.Objects++
		st.Bytes += info.Size()
		return nil
	})
	return st, err
}

// DiskUsage returns used and total bytes for the data root volume (best-effort).
func (s *Store) DiskUsage() (used, total uint64, err error) { return diskUsage(s.root) }

// SetETag preserves multipart ETags on replicas.
func (s *Store) SetETag(polka, key, etag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, err := s.objectPath(polka, key)
	if err != nil {
		return err
	}
	meta, err := s.readSideMeta(op)
	if err != nil {
		return err
	}
	s.prepareCatalogueMutation(polka, key)
	meta.ETag = etag
	if err := s.writeSideMeta(op, meta); err != nil {
		pp, _ := s.polkaPath(polka)
		delete(s.catalogues, pp)
		return err
	}
	s.refreshCatalogue(polka, op, meta)
	return nil
}

// DiskAvailable reports filesystem bytes available to this process.
func (s *Store) DiskAvailable() (uint64, error) { return diskFree(s.root) }
