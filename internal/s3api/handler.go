// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package s3api implements path-style S3-compatible HTTP API.
// In product terms a bucket is a «полка» (shelf).
package s3api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/rkislov/kladovka/internal/auth"
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/metrics"
	"github.com/rkislov/kladovka/internal/storage"
	"github.com/rkislov/kladovka/internal/webui"
)

// Server wires storage, auth and cluster into HTTP handlers.
type Server struct {
	Cfg   *config.Config
	Store *storage.Store
	Auth  *auth.Authenticator
	Rep   *cluster.Replicator
	Reg   *cluster.Registry
}

// Handler returns the root mux with Prometheus metrics middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/internal/health", s.handleInternalHealth)
	mux.HandleFunc("/internal/object/", s.handleInternalObject)
	mux.HandleFunc("/cluster", s.handleCluster)
	mux.Handle("/metrics", metrics.Handler(s.refreshMetrics))
	mux.Handle("/internal/prometheus-sd", metrics.SDHandler(s.Reg, "/metrics"))
	mux.HandleFunc("/api/polki", s.handleAPIPolki)
	mux.HandleFunc("/api/polki/", s.handleAPIPolki)
	mux.Handle("/ui/", webui.Handler())
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.HandleFunc("/", s.handleS3)
	return metrics.Middleware(mux)
}

func (s *Server) refreshMetrics() {
	used, total, _ := s.Store.DiskUsage()
	metrics.UpdateStorage(used, total)
	s.Reg.SetSelfUsed(used)
	for _, n := range s.Reg.Snapshot() {
		metrics.UpdateClusterNode(n.NodeID, n.Healthy, n.UsedBytes)
	}
}

func (s *Server) handleAPIPolki(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		names, err := s.Store.ListPolki()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"polki": names})
	case http.MethodPost:
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Name) == "" {
			http.Error(w, "нужно поле name", http.StatusBadRequest)
			return
		}
		if err := s.Store.CreatePolka(body.Name); err != nil {
			if errors.Is(err, storage.ErrPolkaExists) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"polka": body.Name})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"service": "kladovka",
		"node_id": s.Cfg.NodeID,
	})
}

func (s *Server) handleInternalHealth(w http.ResponseWriter, r *http.Request) {
	used, _, _ := s.Store.DiskUsage()
	s.Reg.SetSelfUsed(used)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"node_id":    s.Cfg.NodeID,
		"healthy":    true,
		"used_bytes": used,
	})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	used, _, _ := s.Store.DiskUsage()
	s.Reg.SetSelfUsed(used)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"nodes":              s.Reg.Snapshot(),
		"this_node":          s.Cfg.NodeID,
		"replication_factor": s.Cfg.ReplicationFactor,
		"used_bytes":         used,
		"terminology": map[string]string{
			"bucket": "полка",
			"store":  "кладовка",
		},
	})
}

func (s *Server) handleInternalObject(w http.ResponseWriter, r *http.Request) {
	// /internal/object/{polka}/{key...}
	path := strings.TrimPrefix(r.URL.Path, "/internal/object/")
	polka, key, ok := splitPolkaKey(path)
	if !ok {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		ct := r.Header.Get("Content-Type")
		body, err := io.ReadAll(io.LimitReader(r.Body, limitBytes(s.Cfg)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exists, _ := s.Store.PolkaExists(polka)
		if !exists {
			_ = s.Store.CreatePolka(polka)
		}
		meta, err := s.Store.PutObject(polka, key, bytes.NewReader(body), ct)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", meta.ETag)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		rc, meta, err := s.Store.GetObject(polka, key)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotFound) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rc.Close()
		w.Header().Set("ETag", meta.ETag)
		w.Header().Set("Content-Type", meta.ContentType)
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.Copy(w, rc)
	case http.MethodDelete:
		_ = s.Store.DeleteObject(polka, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	_, err := s.Auth.AuthenticateRequest(r, r.Header.Get("X-Amz-Content-Sha256"))
	if err == nil {
		return true
	}
	if errors.Is(err, auth.ErrMissingAuth) {
		// Allow unauthenticated health-like paths only; S3 needs auth.
		writeS3Error(w, http.StatusForbidden, "AccessDenied", err.Error(), r.URL.Path)
		return false
	}
	code := "AccessDenied"
	if errors.Is(err, auth.ErrUnknownKey) {
		code = "InvalidAccessKeyId"
	} else if errors.Is(err, auth.ErrBadSignature) {
		code = "SignatureDoesNotMatch"
	} else if errors.Is(err, auth.ErrExpired) {
		code = "AccessDenied"
	}
	writeS3Error(w, http.StatusForbidden, code, err.Error(), r.URL.Path)
	return false
}

func (s *Server) handleS3(w http.ResponseWriter, r *http.Request) {
	// Skip auth for already-handled prefixes (mux may still hit / for some cases).
	if strings.HasPrefix(r.URL.Path, "/internal/") ||
		strings.HasPrefix(r.URL.Path, "/ui") ||
		strings.HasPrefix(r.URL.Path, "/api/") ||
		r.URL.Path == "/healthz" ||
		r.URL.Path == "/cluster" ||
		r.URL.Path == "/metrics" {
		http.NotFound(w, r)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		s.handleListPolki(w, r)
		return
	}
	polka, key, hasKey := splitPolkaKey(path)
	if !hasKey {
		s.handlePolka(w, r, polka)
		return
	}
	s.handleObject(w, r, polka, key)
}

func (s *Server) handleListPolki(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed", "/")
		return
	}
	names, err := s.Store.ListPolki()
	if err != nil {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, "/")
		return
	}
	metrics.RecordS3Operation("ListBuckets", "_")
	writeXML(w, http.StatusOK, listBucketsXML(names))
}

func (s *Server) handlePolka(w http.ResponseWriter, r *http.Request, polka string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if err := s.Store.CreatePolka(polka); err != nil {
			st, code, msg := mapStorageErr(err)
			if errors.Is(err, storage.ErrPolkaExists) {
				w.WriteHeader(http.StatusOK)
				return
			}
			writeS3Error(w, st, code, msg, "/"+polka)
			return
		}
		metrics.RecordS3Operation("CreateBucket", polka)
		w.WriteHeader(http.StatusOK)
	case http.MethodHead:
		ok, err := s.Store.PolkaExists(polka)
		if err != nil || !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified shelf (полка) does not exist", "/"+polka)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := s.Store.DeletePolka(polka); err != nil {
			st, code, msg := mapStorageErr(err)
			writeS3Error(w, st, code, msg, "/"+polka)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		// ListObjectsV2 when list-type=2 or default list.
		prefix := q.Get("prefix")
		maxKeys := 1000
		if v := q.Get("max-keys"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				maxKeys = n
			}
		}
		startAfter := q.Get("continuation-token")
		if startAfter == "" {
			startAfter = q.Get("start-after")
		}
		objs, truncated, err := s.Store.ListObjects(polka, prefix, maxKeys, startAfter)
		if err != nil {
			st, code, msg := mapStorageErr(err)
			writeS3Error(w, st, code, msg, "/"+polka)
			return
		}
		metrics.RecordS3Operation("ListObjectsV2", polka)
		writeXML(w, http.StatusOK, listObjectsXML(polka, prefix, startAfter, maxKeys, objs, truncated))
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed", "/"+polka)
	}
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	if s.handleObjectMultipart(w, r, polka, key) {
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.putObject(w, r, polka, key)
	case http.MethodGet:
		s.getObject(w, r, polka, key)
	case http.MethodHead:
		s.headObject(w, r, polka, key)
	case http.MethodDelete:
		s.deleteObject(w, r, polka, key)
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed", r.URL.Path)
	}
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	if s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		// Forward to primary.
		s.forwardToPrimary(w, r, polka, key)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limitBytes(s.Cfg)))
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), r.URL.Path)
		return
	}
	if s.Cfg.MaxObjectSize > 0 && int64(len(body)) > s.Cfg.MaxObjectSize {
		writeS3Error(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "object too large", r.URL.Path)
		return
	}
	exists, _ := s.Store.PolkaExists(polka)
	if !exists {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified shelf (полка) does not exist", "/"+polka)
		return
	}
	ct := r.Header.Get("Content-Type")
	meta, err := s.Store.PutObject(polka, key, bytes.NewReader(body), ct)
	if err != nil {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	if s.Cfg.ReplicationFactor > 1 {
		if err := s.Rep.ReplicatePut(r.Context(), polka, key, body, ct); err != nil {
			metrics.RecordReplicationFailed()
		} else {
			metrics.RecordReplicationOK()
			metrics.RecordReplicationPublished(s.Cfg.ReplicationFactor - 1)
		}
	}
	metrics.RecordS3Operation("PutObject", polka)
	metrics.RecordBytesUploaded(polka, int64(len(body)))
	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	rc, meta, err := s.Store.GetObject(polka, key)
	if err == nil {
		defer rc.Close()
		w.Header().Set("ETag", meta.ETag)
		w.Header().Set("Content-Type", meta.ContentType)
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
		w.Header().Set("x-amz-kladovka-polka", polka)
		n, _ := io.Copy(w, rc)
		metrics.RecordS3Operation("GetObject", polka)
		metrics.RecordBytesDownloaded(polka, n)
		return
	}
	if !errors.Is(err, storage.ErrObjectNotFound) {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	// Try peers if not found locally.
	if s.Cfg.ReplicationFactor > 1 {
		if s.tryFetchFromPeers(w, r, polka, key) {
			return
		}
	}
	writeS3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist", r.URL.Path)
}

func (s *Server) headObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	meta, err := s.Store.HeadObject(polka, key)
	if err != nil {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
	metrics.RecordS3Operation("HeadObject", polka)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	if s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		s.forwardToPrimary(w, r, polka, key)
		return
	}
	err := s.Store.DeleteObject(polka, key)
	if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	if s.Cfg.ReplicationFactor > 1 {
		s.Rep.ReplicateDelete(r.Context(), polka, key)
	}
	metrics.RecordS3Operation("DeleteObject", polka)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) forwardToPrimary(w http.ResponseWriter, r *http.Request, polka, key string) {
	base := s.Rep.PrimaryURL(polka, key)
	if base == "" || base == s.Cfg.NodeURL {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "primary unavailable", r.URL.Path)
		return
	}
	u := strings.TrimRight(base, "/") + "/" + url.PathEscape(polka) + "/" + escapeKeyPath(key)
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, limitBytes(s.Cfg)))
	req, err := http.NewRequestWithContext(r.Context(), r.Method, u, bytes.NewReader(body))
	if err != nil {
		writeS3Error(w, http.StatusBadGateway, "InternalError", err.Error(), r.URL.Path)
		return
	}
	for k, vv := range r.Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	resp, err := s.Reg.Client().Do(req)
	if err != nil {
		writeS3Error(w, http.StatusBadGateway, "InternalError", err.Error(), r.URL.Path)
		return
	}
	defer resp.Body.Close()
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) tryFetchFromPeers(w http.ResponseWriter, r *http.Request, polka, key string) bool {
	for _, nid := range s.Rep.Placement(polka, key) {
		if nid == s.Reg.SelfID() {
			continue
		}
		base := s.Reg.GetURL(nid)
		if base == "" {
			continue
		}
		u := base + "/internal/object/" + url.PathEscape(polka) + "/" + escapeKeyPath(key)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := s.Reg.Client().Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			continue
		}
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, resp.Body)
		_ = resp.Body.Close()
		return true
	}
	return false
}

func splitPolkaKey(path string) (polka, key string, hasKey bool) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", "", false
	}
	i := strings.IndexByte(path, '/')
	if i < 0 {
		return path, "", false
	}
	polka = path[:i]
	key, _ = url.PathUnescape(path[i+1:])
	return polka, key, true
}

func escapeKeyPath(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func limitBytes(cfg *config.Config) int64 {
	if cfg.MaxObjectSize > 0 {
		return cfg.MaxObjectSize + 1
	}
	return 32 << 30 // 32 GiB soft cap for ReadAll paths
}
