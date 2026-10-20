// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.

package storage

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrIntegrity    = errors.New("object integrity check failed")
	ErrPendingWrite = errors.New("object has an unfinished durable write; restart or retry after restoring storage")
)

type transaction struct {
	Polka  string      `json:"polka"`
	Key    string      `json:"key"`
	Delete bool        `json:"delete,omitempty"`
	Meta   *ObjectMeta `json:"meta,omitempty"`
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func durableMkdir(path, root string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	for dir := path; ; dir = filepath.Dir(dir) {
		if err := syncDir(dir); err != nil {
			return err
		}
		if dir == root {
			return syncDir(filepath.Dir(root))
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	return nil
}
func writeAtomic(path string, body []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".kladovka-meta-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(body); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
func (s *Store) transactionRoot() string { return filepath.Join(s.root, ".transactions") }
func (s *Store) step(name string) error {
	if s.afterStep != nil {
		return s.afterStep(name)
	}
	return nil
}

// putLocked prepares a durable intent before changing the public object path.
// The caller holds mu; readers never see a data/metadata pair midway through commit.
func (s *Store) putLocked(polka, key string, body io.Reader, contentType, etag string) (*ObjectMeta, error) {
	if err := s.recoverPending(); err != nil {
		return nil, err
	}
	if _, err := s.objectPath(polka, key); err != nil {
		return nil, err
	}
	if ok, err := s.PolkaExists(polka); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrPolkaNotFound
	}
	dir, err := os.MkdirTemp(s.transactionRoot(), "write-")
	if err != nil {
		return nil, err
	}
	prepared := false
	defer func() {
		if !prepared {
			os.RemoveAll(dir)
		}
	}()
	f, err := os.OpenFile(filepath.Join(dir, "data"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	md, sha := md5.New(), sha256.New()
	n, err := s.copyBounded(io.MultiWriter(f, md, sha), body)
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return nil, err
	}
	if etag == "" {
		etag = `"` + hex.EncodeToString(md.Sum(nil)) + `"`
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	meta := &ObjectMeta{Key: key, Size: n, ETag: etag, SHA256: hex.EncodeToString(sha.Sum(nil)), ContentType: contentType, LastModified: time.Now().UTC()}
	tx := transaction{Polka: polka, Key: key, Meta: meta}
	b, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}
	if err = writeAtomic(filepath.Join(dir, "manifest.json"), b); err != nil {
		return nil, err
	}
	if err = syncDir(s.transactionRoot()); err != nil {
		return nil, err
	}
	prepared = true
	if err = s.commitTransaction(dir, tx, true); err != nil {
		s.pending[polka+"/"+key] = true
		return nil, err
	}
	return meta, nil
}
func (s *Store) commitTransaction(dir string, tx transaction, inject bool) error {
	if !s.catalogueExactNames {
		clear(s.catalogues)
	}
	complete := false
	defer func() {
		if !complete {
			pp, _ := s.polkaPath(tx.Polka)
			delete(s.catalogues, pp)
		}
	}()
	op, err := s.objectPath(tx.Polka, tx.Key)
	if err != nil {
		return err
	}
	if inject {
		if err = s.step("prepared"); err != nil {
			return err
		}
	}
	if tx.Delete {
		if e := os.Remove(op); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if err = syncDir(filepath.Dir(op)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if inject {
			if err = s.step("data-published"); err != nil {
				return err
			}
		}
		if e := os.Remove(op + ".kladovka-meta.json"); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if err = syncDir(filepath.Dir(op)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		if tx.Meta == nil || tx.Meta.Key != tx.Key || len(tx.Meta.SHA256) != 64 {
			return ErrIntegrity
		}
		if _, err = hex.DecodeString(tx.Meta.SHA256); err != nil {
			return ErrIntegrity
		}
		if err = durableMkdir(filepath.Dir(op), s.root); err != nil {
			return err
		}
		staged := filepath.Join(dir, "data")
		if _, e := os.Stat(staged); e == nil {
			if err = os.Rename(staged, op); err != nil {
				return err
			}
			if err = syncDir(filepath.Dir(op)); err != nil {
				return err
			}
			if err = syncDir(dir); err != nil {
				return err
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if inject {
			if err = s.step("data-published"); err != nil {
				return err
			}
		}
		if err = verifyFile(op, tx.Meta.SHA256); err != nil {
			return err
		}
		if err = s.writeSideMeta(op, tx.Meta); err != nil {
			return err
		}
	}
	if inject {
		if err = s.step("metadata-published"); err != nil {
			return err
		}
	}
	if err = os.RemoveAll(dir); err != nil {
		return err
	}
	if err = syncDir(s.transactionRoot()); err != nil {
		return err
	}
	if tx.Delete {
		s.pruneEmptyParents(tx.Polka, op)
	}
	if tx.Delete {
		s.refreshCatalogue(tx.Polka, op, nil)
	} else {
		s.refreshCatalogue(tx.Polka, op, tx.Meta)
	}
	complete = true
	return nil
}
func verifyFile(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return fmt.Errorf("%w: %s", ErrIntegrity, filepath.Base(path))
	}
	return nil
}
func (s *Store) recoverPending() error {
	if len(s.pending) == 0 {
		return nil
	}
	return s.recoverTransactions()
}
func (s *Store) recoverTransactions() error {
	entries, err := os.ReadDir(s.transactionRoot())
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(s.transactionRoot(), entry.Name())
		body, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if errors.Is(e, os.ErrNotExist) {
			if e = os.RemoveAll(dir); e != nil {
				return e
			}
			continue
		}
		if e != nil {
			return e
		}
		var tx transaction
		if e = json.Unmarshal(body, &tx); e != nil {
			return fmt.Errorf("invalid transaction %s: %w", entry.Name(), e)
		}
		if e = s.commitTransaction(dir, tx, false); e != nil {
			return fmt.Errorf("recover transaction %s: %w", entry.Name(), e)
		}
	}
	s.pending = map[string]bool{}
	return syncDir(s.transactionRoot())
}
func (s *Store) deleteLocked(polka, key string) error {
	if err := s.recoverPending(); err != nil {
		return err
	}
	op, err := s.objectPath(polka, key)
	if err != nil {
		return err
	}
	if _, err = os.Stat(op); errors.Is(err, os.ErrNotExist) {
		return ErrObjectNotFound
	} else if err != nil {
		return err
	}
	dir, err := os.MkdirTemp(s.transactionRoot(), "delete-")
	if err != nil {
		return err
	}
	tx := transaction{Polka: polka, Key: key, Delete: true}
	body, _ := json.Marshal(tx)
	if err = writeAtomic(filepath.Join(dir, "manifest.json"), body); err != nil {
		os.RemoveAll(dir)
		return err
	}
	if err = syncDir(s.transactionRoot()); err != nil {
		return err
	}
	if err = s.commitTransaction(dir, tx, true); err != nil {
		s.pending[polka+"/"+key] = true
	}
	return err
}
func (s *Store) pruneEmptyParents(polka, op string) {
	pp, _ := s.polkaPath(polka)
	for dir := filepath.Dir(op); dir != pp && strings.HasPrefix(dir, pp+string(filepath.Separator)); dir = filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			break
		}
		_ = syncDir(filepath.Dir(dir))
	}
}
