// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNoSuchUpload = errors.New("нет такой multipart-загрузки")
)

// MultipartPart describes one uploaded part.
type MultipartPart struct {
	PartNumber   int       `json:"part_number"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

type multipartMeta struct {
	UploadID    string    `json:"upload_id"`
	Polka       string    `json:"polka"`
	Key         string    `json:"key"`
	ContentType string    `json:"content_type"`
	CreatedAt   time.Time `json:"created_at"`
}

func (s *Store) multipartRoot() string {
	return filepath.Join(s.root, ".multipart")
}

func (s *Store) multipartDir(uploadID string) string {
	return filepath.Join(s.multipartRoot(), uploadID)
}

// CreateMultipartUpload starts a multipart upload and returns upload id.
func (s *Store) CreateMultipartUpload(polka, key, contentType string) (string, error) {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, err := s.PolkaExists(polka)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrPolkaNotFound
	}
	if _, err := s.objectPath(polka, key); err != nil {
		return "", err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(raw[:])
	dir := s.multipartDir(id)
	if err := durableMkdir(dir, s.root); err != nil {
		return "", err
	}
	meta := multipartMeta{
		UploadID:    id,
		Polka:       polka,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   time.Now().UTC(),
	}
	b, _ := json.Marshal(meta)
	if err := writeAtomic(filepath.Join(dir, "meta.json"), b); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) loadMultipart(uploadID string) (*multipartMeta, error) {
	if len(uploadID) != 32 {
		return nil, ErrNoSuchUpload
	}
	if _, err := hex.DecodeString(uploadID); err != nil {
		return nil, ErrNoSuchUpload
	}
	b, err := os.ReadFile(filepath.Join(s.multipartDir(uploadID), "meta.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoSuchUpload
	}
	if err != nil {
		return nil, err
	}
	var m multipartMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// UploadPart stores one part of a multipart upload.
func (s *Store) UploadPart(uploadID string, partNumber int, body io.Reader) (*MultipartPart, error) {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if partNumber < 1 || partNumber > 10000 {
		return nil, fmt.Errorf("некорректный номер части")
	}
	if _, err := s.loadMultipart(uploadID); err != nil {
		return nil, err
	}
	dir := s.multipartDir(uploadID)
	partPath := filepath.Join(dir, fmt.Sprintf("part-%05d", partNumber))
	tmp := partPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	h := md5.New()
	n, err := s.copyBounded(io.MultiWriter(f, h), body)
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, partPath); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err = syncDir(dir); err != nil {
		return nil, err
	}
	etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
	return &MultipartPart{PartNumber: partNumber, ETag: etag, Size: n}, nil
}

// ListParts returns uploaded parts for an upload id.
func (s *Store) ListParts(uploadID string) ([]MultipartPart, error) {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	return s.listParts(uploadID)
}
func (s *Store) listParts(uploadID string) ([]MultipartPart, error) {
	if _, err := s.loadMultipart(uploadID); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.multipartDir(uploadID))
	if err != nil {
		return nil, err
	}
	var parts []MultipartPart
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "part-") {
			continue
		}
		numStr := strings.TrimPrefix(name, "part-")
		n, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		// Recompute etag lightly from file md5 for listing.
		f, err := os.Open(filepath.Join(s.multipartDir(uploadID), name))
		if err != nil {
			continue
		}
		h := md5.New()
		_, _ = io.Copy(h, f)
		_ = f.Close()
		parts = append(parts, MultipartPart{
			PartNumber:   n,
			ETag:         `"` + hex.EncodeToString(h.Sum(nil)) + `"`,
			LastModified: info.ModTime().UTC(),
			Size:         info.Size(),
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

// CompleteMultipartUpload concatenates parts into the final object.
func (s *Store) CompleteMultipartUpload(uploadID string, parts []MultipartPart) (*ObjectMeta, error) {
	meta, err := s.AssembleMultipartUpload(uploadID, parts)
	if err != nil {
		return nil, err
	}
	if err = s.AbortMultipartUpload(uploadID); err != nil {
		return nil, err
	}
	return meta, nil
}

// AssembleMultipartUpload retains parts until the caller confirms replication.
func (s *Store) AssembleMultipartUpload(uploadID string, parts []MultipartPart) (*ObjectMeta, error) {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	meta, err := s.loadMultipart(uploadID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok, err := s.PolkaExists(meta.Polka); err != nil || !ok {
		return nil, ErrPolkaNotFound
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("нет частей")
	}
	available, err := s.listParts(uploadID)
	if err != nil {
		return nil, err
	}
	byNumber := map[int]MultipartPart{}
	for _, p := range available {
		byNumber[p.PartNumber] = p
	}
	composite := md5.New()
	previous := 0
	for _, p := range parts {
		got, ok := byNumber[p.PartNumber]
		if !ok || p.PartNumber <= previous || (p.ETag != got.ETag && `"`+p.ETag+`"` != got.ETag) {
			return nil, fmt.Errorf("invalid part order or ETag")
		}
		previous = p.PartNumber
		raw, _ := hex.DecodeString(strings.Trim(got.ETag, `"`))
		composite.Write(raw)
	}
	paths := make([]string, 0, len(parts))
	for _, p := range parts {
		paths = append(paths, filepath.Join(s.multipartDir(uploadID), fmt.Sprintf("part-%05d", p.PartNumber)))
	}
	reader := &partReader{paths: paths}
	defer reader.Close()
	etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(composite.Sum(nil)), len(parts))
	return s.putLocked(meta.Polka, meta.Key, reader, meta.ContentType, etag)
}

// AbortMultipartUpload removes an in-progress upload.
func (s *Store) AbortMultipartUpload(uploadID string) error {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.loadMultipart(uploadID); err != nil {
		return err
	}
	if err := os.RemoveAll(s.multipartDir(uploadID)); err != nil {
		return err
	}
	return syncDir(s.multipartRoot())
}

// ValidateMultipartTarget prevents an upload id being used with a different bucket/key.
func (s *Store) ValidateMultipartTarget(id, polka, key string) error {
	m, err := s.loadMultipart(id)
	if err != nil {
		return err
	}
	if m.Polka != polka || m.Key != key {
		return ErrNoSuchUpload
	}
	return nil
}

// partReader opens one part at a time, even for uploads with thousands of parts.
type partReader struct {
	paths   []string
	current *os.File
	index   int
}

func (r *partReader) Read(body []byte) (int, error) {
	for {
		if r.current == nil {
			if r.index >= len(r.paths) {
				return 0, io.EOF
			}
			f, err := os.Open(r.paths[r.index])
			if err != nil {
				return 0, err
			}
			r.current = f
			r.index++
		}
		n, err := r.current.Read(body)
		if err == io.EOF {
			r.current.Close()
			r.current = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}
func (r *partReader) Close() error {
	if r.current != nil {
		return r.current.Close()
	}
	return nil
}

// PinMultipart prevents TTL cleanup while an HTTP operation (including replication) is active.
func (s *Store) PinMultipart(id string) (func(), error) {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	if _, err := s.loadMultipart(id); err != nil {
		return nil, err
	}
	s.active[id]++
	return func() {
		s.multipartMu.Lock()
		defer s.multipartMu.Unlock()
		s.active[id]--
		if s.active[id] == 0 {
			delete(s.active, id)
		}
	}, nil
}

// MultipartTarget resolves staged data without exposing its on-disk metadata.
func (s *Store) MultipartTarget(id string) (string, string, error) {
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	m, err := s.loadMultipart(id)
	if err != nil {
		return "", "", err
	}
	return m.Polka, m.Key, nil
}
