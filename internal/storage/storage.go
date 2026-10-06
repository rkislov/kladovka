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
	"crypto/md5"
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
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	ContentType  string    `json:"content_type"`
	LastModified time.Time `json:"last_modified"`
}

// Store is a filesystem-backed object store. Polka = S3 bucket directory.
type Store struct {
	root string
	mu   sync.RWMutex
}

// New creates a store under root and ensures the directory exists.
func New(root string) (*Store, error) {
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

func (s *Store) polkaPath(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("некорректное имя полки")
	}
	return filepath.Join(s.root, name), nil
}

func (s *Store) objectPath(polka, key string) (string, error) {
	pp, err := s.polkaPath(polka)
	if err != nil {
		return "", err
	}
	key = strings.TrimPrefix(key, "/")
	if key == "" || strings.Contains(key, "..") {
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
	return os.MkdirAll(pp, 0o755)
}

// DeletePolka removes an empty shelf.
func (s *Store) DeletePolka(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	return os.Remove(pp)
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
	ok, err := s.PolkaExists(polka)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrPolkaNotFound
	}
	op, err := s.objectPath(polka, key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(op), 0o755); err != nil {
		return nil, err
	}

	tmp := op + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	h := md5.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, op); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	meta := &ObjectMeta{
		Key:          key,
		Size:         n,
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
	}
	_ = s.writeSideMeta(op, meta)
	return meta, nil
}

func (s *Store) writeSideMeta(objectPath string, meta *ObjectMeta) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(objectPath+".kladovka-meta.json", b, 0o644)
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
	op, err := s.objectPath(polka, key)
	if err != nil {
		return nil, nil, err
	}
	st, err := os.Stat(op)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(op)
	if err != nil {
		return nil, nil, err
	}
	meta, err := s.readSideMeta(op)
	if err != nil {
		// Fall back to filesystem stats.
		h := md5.New()
		_, _ = io.Copy(h, f)
		_, _ = f.Seek(0, io.SeekStart)
		meta = &ObjectMeta{
			Key:          key,
			Size:         st.Size(),
			ETag:         `"` + hex.EncodeToString(h.Sum(nil)) + `"`,
			ContentType:  "application/octet-stream",
			LastModified: st.ModTime().UTC(),
		}
	} else {
		meta.Size = st.Size()
		meta.LastModified = st.ModTime().UTC()
	}
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
	op, err := s.objectPath(polka, key)
	if err != nil {
		return err
	}
	if _, err := os.Stat(op); errors.Is(err, os.ErrNotExist) {
		return ErrObjectNotFound
	}
	_ = os.Remove(op + ".kladovka-meta.json")
	return os.Remove(op)
}

// ListObjects lists objects under a shelf with optional prefix (flat keys).
func (s *Store) ListObjects(polka, prefix string, maxKeys int, startAfter string) ([]ObjectMeta, bool, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	ok, err := s.PolkaExists(polka)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, ErrPolkaNotFound
	}
	pp, _ := s.polkaPath(polka)
	var all []ObjectMeta
	err = filepath.WalkDir(pp, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".kladovka-meta.json") || strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(pp, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		st, err := d.Info()
		if err != nil {
			return err
		}
		meta, merr := s.readSideMeta(path)
		if merr != nil {
			meta = &ObjectMeta{Key: key, Size: st.Size(), ETag: `""`, ContentType: "application/octet-stream", LastModified: st.ModTime().UTC()}
		} else {
			meta.Key = key
			meta.Size = st.Size()
			meta.LastModified = st.ModTime().UTC()
		}
		all = append(all, *meta)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	out := make([]ObjectMeta, 0, maxKeys)
	for _, m := range all {
		if startAfter != "" && m.Key <= startAfter {
			continue
		}
		out = append(out, m)
		if len(out) >= maxKeys {
			return out, true, nil
		}
	}
	return out, false, nil
}

// PolkaStats is a summary of one shelf for the dashboard.
type PolkaStats struct {
	Name    string `json:"name"`
	Objects int    `json:"objects"`
	Bytes   int64  `json:"bytes"`
}

// StatsForPolka counts objects and bytes on a shelf (skips meta/tmp files).
func (s *Store) StatsForPolka(polka string) (PolkaStats, error) {
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
func (s *Store) DiskUsage() (used, total uint64, err error) {
	var size int64
	_ = filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, e := d.Info()
		if e == nil {
			size += info.Size()
		}
		return nil
	})
	return uint64(size), 0, nil
}
