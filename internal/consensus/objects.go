// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/rkislov/kladovka/internal/storage"
)

func validShelf(name string) bool {
	if name == "" || name != strings.TrimSpace(name) || strings.HasPrefix(name, ".") || strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
		return false
	}
	switch name {
	case "api", "ui", "internal", "healthz", "cluster", "metrics":
		return false
	}
	return true
}
func validKey(key string) bool {
	if key == "" || strings.Contains(key, "..") || strings.HasSuffix(key, ".tmp") || strings.HasSuffix(key, ".kladovka-meta.json") {
		return false
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." {
			return false
		}
	}
	return true
}
func (s *Store) CreatePolka(name string) error {
	if !validShelf(name) {
		return errors.New("invalid shelf")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.apply(command{Kind: "create", Polka: name})
}
func (s *Store) DeletePolka(name string) error {
	if !validShelf(name) {
		return errors.New("invalid shelf")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.apply(command{Kind: "drop", Polka: name})
}
func (s *Store) PolkaExists(name string) (bool, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return s.fsm.state.Buckets[name], nil
}
func (s *Store) ListPolki() ([]string, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	out := []string{}
	for name := range s.fsm.state.Buckets {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}
func (s *Store) PutObject(polka, key string, body io.Reader, contentType string) (*storage.ObjectMeta, error) {
	return s.PutObjectWithETag(polka, key, body, contentType, "")
}
func (s *Store) PutObjectWithETag(polka, key string, body io.Reader, contentType, etag string) (*storage.ObjectMeta, error) {
	if !validShelf(polka) || !validKey(key) {
		return nil, errors.New("invalid object path")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if e := s.Barrier(); e != nil {
		return nil, e
	}
	if ok, _ := s.PolkaExists(polka); !ok {
		return nil, storage.ErrPolkaNotFound
	}
	quotaUsed, _, quotaErr := s.QuotaUsage()
	if quotaErr != nil {
		return nil, quotaErr
	}
	sha, md := sha256.New(), md5.New()
	chunks := []Chunk{}
	var size int64
	buf := make([]byte, ChunkSize)
	for {
		n, e := io.ReadFull(body, buf)
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			return nil, e
		}
		if n == 0 {
			break
		}
		size += int64(n)
		if size > s.cfg.MaxObjectSize {
			return nil, errors.New("object exceeds consensus size limit")
		}
		if s.cfg.StorageQuotaBytes > 0 {
			if quotaUsed+size > s.cfg.StorageQuotaBytes {
				return nil, storage.ErrQuotaExceeded
			}
		}
		if e := s.Store.CheckFreeSpace(3 * ChunkSize); e != nil {
			return nil, e
		}
		data := buf[:n]
		sha.Write(data)
		md.Write(data)
		hash := sha256.Sum256(data)
		h := hex.EncodeToString(hash[:])
		chunks = append(chunks, Chunk{Hash: h, Size: int64(n)})
		s.fsm.mu.RLock()
		_, known := s.fsm.state.Chunks[h]
		s.fsm.mu.RUnlock()
		_, localErr := s.fsm.chunk(h)
		if !known || localErr != nil {
			if e := s.apply(command{Kind: "chunk", Hash: h, Data: data}); e != nil {
				return nil, e
			}
		}
		if e == io.EOF || e == io.ErrUnexpectedEOF {
			break
		}
	}
	if etag == "" {
		etag = `"` + hex.EncodeToString(md.Sum(nil)) + `"`
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	meta := storage.ObjectMeta{Key: key, Size: size, ETag: etag, SHA256: hex.EncodeToString(sha.Sum(nil)), ContentType: contentType, LastModified: time.Now().UTC()}
	if e := s.apply(command{Kind: "put", Record: Record{Polka: polka, Key: key, Meta: meta, Chunks: chunks}}); e != nil {
		return nil, e
	}
	return &meta, nil
}
func (s *Store) DeleteObject(polka, key string) error {
	if !validShelf(polka) || !validKey(key) {
		return errors.New("invalid object path")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.apply(command{Kind: "delete", Polka: polka, Key: key})
}
func (s *Store) record(polka, key string) (Record, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if !s.fsm.state.Buckets[polka] {
		return Record{}, storage.ErrPolkaNotFound
	}
	r, ok := s.fsm.state.Records[polka+"/"+key]
	if !ok || r.Deleted {
		return Record{}, storage.ErrObjectNotFound
	}
	return r, nil
}
func (s *Store) GetObject(polka, key string) (io.ReadCloser, *storage.ObjectMeta, error) {
	if e := s.Barrier(); e != nil {
		return nil, nil, e
	}
	s.fsm.mu.Lock()
	r, ok := s.fsm.state.Records[polka+"/"+key]
	if !s.fsm.state.Buckets[polka] {
		s.fsm.mu.Unlock()
		return nil, nil, storage.ErrPolkaNotFound
	}
	if !ok || r.Deleted {
		s.fsm.mu.Unlock()
		return nil, nil, storage.ErrObjectNotFound
	}
	if s.fsm.snapshotRefs == nil {
		s.fsm.snapshotRefs = map[string]int{}
	}
	hashes := make([]string, 0, len(r.Chunks))
	for _, c := range r.Chunks {
		s.fsm.snapshotRefs[c.Hash]++
		hashes = append(hashes, c.Hash)
	}
	s.fsm.mu.Unlock()
	var e error
	reader := &chunkReader{s: s, chunks: r.Chunks, size: r.Meta.Size, cached: -1, release: func() { s.fsm.releaseChunks(hashes) }}
	// Validate before sending headers, so missing/corrupt data is an explicit error.
	h := sha256.New()
	n, e := io.Copy(h, reader)
	if e != nil {
		reader.Close()
		return nil, nil, e
	}
	if n != r.Meta.Size || hex.EncodeToString(h.Sum(nil)) != r.Meta.SHA256 {
		reader.Close()
		return nil, nil, storage.ErrIntegrity
	}
	reader.Seek(0, io.SeekStart)
	meta := r.Meta
	return reader, &meta, nil
}
func (s *Store) HeadObject(polka, key string) (*storage.ObjectMeta, error) {
	r, m, e := s.GetObject(polka, key)
	if e == nil {
		r.Close()
	}
	return m, e
}
func (s *Store) ListObjects(polka, prefix string, maxKeys int, after string) ([]storage.ObjectMeta, bool, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if !s.fsm.state.Buckets[polka] {
		return nil, false, storage.ErrPolkaNotFound
	}
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	all := []storage.ObjectMeta{}
	for _, r := range s.fsm.state.Records {
		if r.Polka == polka && !r.Deleted && strings.HasPrefix(r.Key, prefix) && r.Key > after {
			all = append(all, r.Meta)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	if len(all) > maxKeys {
		return all[:maxKeys], true, nil
	}
	return all, false, nil
}
func (s *Store) StatsForPolka(polka string) (storage.PolkaStats, error) {
	objs, _, e := s.ListObjects(polka, "", int(^uint(0)>>1), "")
	v := storage.PolkaStats{Name: polka}
	for _, o := range objs {
		v.Objects++
		v.Bytes += o.Size
	}
	return v, e
}
func (s *Store) QuotaUsage() (int64, int64, error) {
	s.fsm.mu.RLock()
	var used int64
	for _, r := range s.fsm.state.Records {
		if !r.Deleted && s.fsm.state.Buckets[r.Polka] {
			used += r.Meta.Size
		}
	}
	s.fsm.mu.RUnlock()
	parts, e := s.Store.MultipartBytes()
	return used + parts, s.cfg.StorageQuotaBytes, e
}

// Multipart staging belongs to the current leader. Only completion is committed.
func (s *Store) CreateMultipartUpload(polka, key, ct string) (string, error) {
	if ok, _ := s.PolkaExists(polka); !ok {
		return "", storage.ErrPolkaNotFound
	}
	if e := s.Store.CreatePolka(polka); e != nil && !errors.Is(e, storage.ErrPolkaExists) {
		return "", e
	}
	return s.Store.CreateMultipartUpload(polka, key, ct)
}
func (s *Store) AssembleMultipartUpload(id string, parts []storage.MultipartPart) (*storage.ObjectMeta, error) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	meta, e := s.Store.AssembleMultipartUpload(id, parts)
	if e != nil {
		return nil, e
	}
	polka, key, e := s.Store.MultipartTarget(id)
	if e != nil {
		return nil, e
	}
	body, _, e := s.Store.GetObject(polka, key)
	if e != nil {
		return nil, e
	}
	defer body.Close()
	return s.PutObjectWithETag(polka, key, body, meta.ContentType, meta.ETag)
}

type chunkReader struct {
	s         *Store
	chunks    []Chunk
	size, pos int64
	cached    int
	data      []byte
	release   func()
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	var start int64
	index := -1
	for i, c := range r.chunks {
		if r.pos < start+c.Size {
			index = i
			break
		}
		start += c.Size
	}
	if index < 0 {
		return 0, storage.ErrIntegrity
	}
	if r.cached != index {
		b, e := r.s.obtain(r.chunks[index].Hash)
		if e != nil {
			return 0, e
		}
		if int64(len(b)) != r.chunks[index].Size {
			return 0, storage.ErrIntegrity
		}
		r.data = b
		r.cached = index
	}
	n := copy(p, r.data[r.pos-start:])
	r.pos += int64(n)
	return n, nil
}
func (r *chunkReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.pos
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, errors.New("invalid seek")
	}
	if offset < 0 {
		return 0, errors.New("negative seek")
	}
	r.pos = offset
	return offset, nil
}
func (r *chunkReader) Close() error {
	if r.release != nil {
		r.release()
		r.release = nil
	}
	r.data = nil
	r.cached = -1
	return nil
}

type partBudget struct {
	reader    io.Reader
	remaining int64
}

func (b *partBudget) Read(p []byte) (int, error) {
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, e := b.reader.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return n, storage.ErrQuotaExceeded
	}
	return n, e
}
func (s *Store) UploadPart(id string, number int, body io.Reader) (*storage.MultipartPart, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if e := s.Barrier(); e != nil {
		return nil, e
	}
	used, total, e := s.QuotaUsage()
	if e != nil {
		return nil, e
	}
	if total > 0 {
		if used >= total {
			return nil, storage.ErrQuotaExceeded
		}
		body = &partBudget{reader: body, remaining: total - used}
	}
	return s.Store.UploadPart(id, number, body)
}
