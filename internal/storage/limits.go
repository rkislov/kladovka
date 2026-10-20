// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package storage

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrQuotaExceeded = errors.New("storage quota exceeded")
var ErrInsufficientSpace = errors.New("disk free-space reserve reached")

type Options struct {
	QuotaBytes   int64
	MinFreeBytes uint64
	VerifyReads  bool
	MultipartTTL time.Duration
}

func NewWithOptions(root string, options Options) (*Store, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if options.QuotaBytes < 0 || options.MultipartTTL < 0 {
		return nil, errors.New("invalid storage limits")
	}
	if err = durableMkdir(root, root); err != nil {
		return nil, err
	}
	s := &Store{root: root, options: options, pending: map[string]bool{}, active: map[string]int{}, catalogues: map[string]*catalogue{}}
	legacy, err := s.legacyFormat()
	if err != nil {
		return nil, err
	}
	if err = durableMkdir(s.transactionRoot(), root); err != nil {
		return nil, err
	}
	if err = s.recoverTransactions(); err != nil {
		return nil, err
	}
	s.catalogueExactNames, err = s.exactCatalogueNames()
	if err != nil {
		return nil, err
	}
	if legacy {
		if err = s.upgradeLegacyMetadata(); err != nil {
			return nil, err
		}
	}
	if err = durableMkdir(s.UploadDir(), root); err != nil {
		return nil, err
	}
	uploads, err := os.ReadDir(s.UploadDir())
	if err != nil {
		return nil, err
	}
	for _, e := range uploads {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "upload-") {
			if err = os.Remove(filepath.Join(s.UploadDir(), e.Name())); err != nil {
				return nil, err
			}
		}
	}
	if err = syncDir(s.UploadDir()); err != nil {
		return nil, err
	}
	return s, nil
}

// logicalBytes counts objects and multipart parts, excluding metadata and staged writes.
func (s *Store) logicalBytes() (int64, error) {
	var used int64
	err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == s.transactionRoot() || path == s.UploadDir() || path == filepath.Join(s.root, ".consensus") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Dir(path) == s.root && strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tmp") || strings.HasSuffix(d.Name(), ".kladovka-meta.json") || (strings.HasPrefix(path, s.multipartRoot()+string(filepath.Separator)) && d.Name() == "meta.json") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		used += info.Size()
		return nil
	})
	return used, err
}

type capacityWriter struct {
	dst       io.Writer
	remaining int64
	store     *Store
}

func (w *capacityWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.remaining {
		return 0, ErrQuotaExceeded
	}
	if w.store.options.MinFreeBytes > 0 {
		free, err := diskFree(w.store.root)
		if err != nil {
			return 0, err
		}
		reserve := w.store.options.MinFreeBytes
		if free < reserve || uint64(len(b)) > free-reserve {
			return 0, ErrInsufficientSpace
		}
	}
	n, err := w.dst.Write(b)
	w.remaining -= int64(n)
	return n, err
}

// The caller holds mu so capacity checks and final publications cannot race.
func (s *Store) copyBounded(dst io.Writer, src io.Reader) (int64, error) {
	if err := s.CheckFreeSpace(0); err != nil {
		return 0, err
	}
	remaining := int64(math.MaxInt64)
	if s.options.QuotaBytes > 0 {
		used, err := s.logicalBytes()
		if err != nil {
			return 0, err
		}
		remaining = s.options.QuotaBytes - used
		if remaining < 0 {
			return 0, ErrQuotaExceeded
		}
	}
	return io.Copy(&capacityWriter{dst: dst, remaining: remaining, store: s}, src)
}

// CleanupMultipart only removes uploads idle for the configured TTL. Multipart
// mutators and cleanup share a lock, so an active part is never removed mid-write.
func (s *Store) CleanupMultipart(now time.Time) (int, error) {
	if s.options.MultipartTTL <= 0 {
		return 0, nil
	}
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.multipartRoot())
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, e := range entries {
		if !e.IsDir() || s.active[e.Name()] > 0 {
			continue
		}
		dir := filepath.Join(s.multipartRoot(), e.Name())
		latest := time.Time{}
		err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if info.ModTime().After(latest) {
				latest = info.ModTime()
			}
			return nil
		})
		if err != nil {
			return removed, err
		}
		if now.Sub(latest) >= s.options.MultipartTTL {
			if err = os.RemoveAll(dir); err != nil {
				return removed, err
			}
			removed++
		}
	}
	if removed > 0 {
		err = syncDir(s.multipartRoot())
	}
	return removed, err
}

// UploadDir is private staging space on the data volume, cleared before startup.
func (s *Store) UploadDir() string { return filepath.Join(s.root, ".uploads") }
func (s *Store) CheckFreeSpace(bytes uint64) error {
	if s.options.MinFreeBytes == 0 {
		return nil
	}
	free, err := diskFree(s.root)
	if err != nil {
		return err
	}
	if free < s.options.MinFreeBytes || bytes > free-s.options.MinFreeBytes {
		return ErrInsufficientSpace
	}
	return nil
}

func (s *Store) QuotaUsage() (int64, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.options.QuotaBytes == 0 {
		return 0, 0, nil
	}
	used, err := s.logicalBytes()
	return used, s.options.QuotaBytes, err
}

// MultipartBytes counts retained parts independently of materialized objects.
func (s *Store) MultipartBytes() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var used int64
	err := filepath.WalkDir(s.multipartRoot(), func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), "part-") && !strings.HasSuffix(d.Name(), ".tmp") {
			info, e := d.Info()
			if e != nil {
				return e
			}
			used += info.Size()
		}
		return nil
	})
	return used, err
}
