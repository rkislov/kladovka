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
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
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
	ok, err := s.PolkaExists(polka)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrPolkaNotFound
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
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
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), b, 0o644); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) loadMultipart(uploadID string) (*multipartMeta, error) {
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
	n, err := io.Copy(io.MultiWriter(f, h), body)
	_ = f.Close()
	if err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, partPath); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
	return &MultipartPart{PartNumber: partNumber, ETag: etag, Size: n}, nil
}

// ListParts returns uploaded parts for an upload id.
func (s *Store) ListParts(uploadID string) ([]MultipartPart, error) {
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
			PartNumber: n,
			ETag:       `"` + hex.EncodeToString(h.Sum(nil)) + `"`,
			Size:       info.Size(),
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

// CompleteMultipartUpload concatenates parts into the final object.
func (s *Store) CompleteMultipartUpload(uploadID string, parts []MultipartPart) (*ObjectMeta, error) {
	meta, err := s.loadMultipart(uploadID)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("нет частей")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	op, err := s.objectPath(meta.Polka, meta.Key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(op), 0o755); err != nil {
		return nil, err
	}
	tmp := op + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	h := md5.New()
	var total int64
	for _, p := range parts {
		partPath := filepath.Join(s.multipartDir(uploadID), fmt.Sprintf("part-%05d", p.PartNumber))
		f, err := os.Open(partPath)
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return nil, fmt.Errorf("часть %d: %w", p.PartNumber, err)
		}
		n, err := io.Copy(io.MultiWriter(out, h), f)
		_ = f.Close()
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return nil, err
		}
		total += n
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, op); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	obj := &ObjectMeta{
		Key:          meta.Key,
		Size:         total,
		ETag:         `"` + hex.EncodeToString(h.Sum(nil)) + `"`,
		ContentType:  meta.ContentType,
		LastModified: time.Now().UTC(),
	}
	_ = s.writeSideMeta(op, obj)
	_ = os.RemoveAll(s.multipartDir(uploadID))
	return obj, nil
}

// AbortMultipartUpload removes an in-progress upload.
func (s *Store) AbortMultipartUpload(uploadID string) error {
	if _, err := s.loadMultipart(uploadID); err != nil {
		return err
	}
	return os.RemoveAll(s.multipartDir(uploadID))
}
