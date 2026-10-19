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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const formatName = ".kladovka-format.json"

func (s *Store) legacyFormat() (bool, error) {
	body, err := os.ReadFile(filepath.Join(s.root, formatName))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var format struct {
		Version int `json:"version"`
	}
	if err = json.Unmarshal(body, &format); err != nil {
		return false, err
	}
	if format.Version != 2 {
		return false, fmt.Errorf("unsupported storage format %d", format.Version)
	}
	return false, nil
}

// Upgrade once before serving, so missing metadata can never silently disable
// integrity checks on objects written by this version. Legacy files without a
// whole-file checksum acquire a baseline from their current bytes.
func (s *Store) upgradeLegacyMetadata() error {
	err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == s.root {
			return nil
		}
		relative, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		segments := strings.Split(filepath.ToSlash(relative), "/")
		if strings.HasPrefix(segments[0], ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() || strings.HasSuffix(d.Name(), ".tmp") || strings.HasSuffix(d.Name(), ".kladovka-meta.json") {
			return nil
		}
		if len(segments) < 2 {
			return fmt.Errorf("unexpected file outside a shelf: %s", relative)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		md, sha := md5.New(), sha256.New()
		n, err := io.Copy(io.MultiWriter(md, sha), f)
		f.Close()
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		sum := hex.EncodeToString(sha.Sum(nil))
		etag := `"` + hex.EncodeToString(md.Sum(nil)) + `"`
		meta, e := s.readSideMeta(path)
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return fmt.Errorf("%w: invalid legacy metadata", ErrIntegrity)
		}
		key := strings.Join(segments[1:], "/")
		if meta == nil {
			meta = &ObjectMeta{Key: key, Size: n, ETag: etag, ContentType: "application/octet-stream", LastModified: info.ModTime().UTC()}
		}
		if meta.Size != n || (meta.SHA256 != "" && meta.SHA256 != sum) || (meta.SHA256 == "" && !strings.Contains(meta.ETag, "-") && meta.ETag != etag) {
			return fmt.Errorf("%w: legacy %s", ErrIntegrity, relative)
		}
		meta.SHA256 = sum
		meta.Key = key
		return s.writeSideMeta(path, meta)
	})
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(s.root, formatName), []byte("{\"version\":2}\n"))
}
