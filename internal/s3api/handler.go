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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/auth"
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/consensus"
	"github.com/rkislov/kladovka/internal/metrics"
	"github.com/rkislov/kladovka/internal/storage"
	"github.com/rkislov/kladovka/internal/webui"
)

// Server wires storage, auth and cluster into HTTP handlers.
type Server struct {
	Cfg          *config.Config
	Store        Backend
	Consensus    *consensus.Store
	Auth         *auth.Authenticator
	Rep          *cluster.Replicator
	Reg          *cluster.Registry
	uploadOnce   sync.Once
	uploadSlots  chan struct{}
	tempMu       sync.Mutex
	tempBytes    int64
	Audit        *audit.Log
	sessionMu    sync.Mutex
	sessions     map[[32]byte]browserSession
	loginWindows map[string]loginWindow
	policyMu     sync.Mutex
	policySeen   bool
	policyHash   [32]byte
}

// Handler returns the root mux with Prometheus metrics middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/internal/health", s.handleInternalHealth)
	if s.Consensus != nil {
		mux.HandleFunc("/internal/consensus/chunk/{hash}", s.Consensus.ChunkHandler)
		mux.HandleFunc("/internal/consensus/candidate", s.Consensus.CandidateHandler)
		mux.HandleFunc("/internal/consensus/ready", s.Consensus.ReadyHandler)
		mux.HandleFunc("/api/cluster/add", s.handleMembership)
		mux.HandleFunc("/api/cluster/remove", s.handleMembership)
		mux.HandleFunc("/api/cluster/replace", s.handleReplaceNode)
		mux.HandleFunc("/api/cluster/maintenance", s.handleMaintenance)
	}
	mux.HandleFunc("/internal/object/", s.handleInternalObject)
	mux.HandleFunc("/internal/delete-primary/", s.handlePrimaryDelete)
	mux.HandleFunc("/internal/polki/", s.handleInternalPolka)
	mux.HandleFunc("/cluster", s.handleCluster)
	mux.Handle("/metrics", metrics.Handler(s.refreshMetrics))
	mux.Handle("/internal/prometheus-sd", metrics.SDHandler(s.Reg, "/metrics"))
	mux.HandleFunc("/api/polki", s.handleAPIPolki)
	mux.HandleFunc("/api/polki/", s.handleAPIPolki)
	mux.HandleFunc("/api/stats", s.handleAPIStats)
	mux.HandleFunc("/api/session", s.handleSession)
	mux.HandleFunc("/api/object/", s.handleAPIObject)
	mux.Handle("/ui/", webui.Handler())
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.HandleFunc("/", s.handleS3)
	return metrics.Middleware(s.auditRequests(s.protect(s.consensusRouting(s.limitUploads(mux)))))
}

func (s *Server) refreshMetrics() {
	used, total, diskErr := s.Store.DiskUsage()
	metrics.UpdateStorage(used, total)
	available := uint64(0)
	valid := false
	if disk, ok := s.Store.(interface{ DiskAvailable() (uint64, error) }); ok {
		var err error
		available, err = disk.DiskAvailable()
		valid = err == nil && diskErr == nil && total > 0
	}
	metrics.UpdateDiskObservation(available, valid)
	if s.Consensus != nil {
		s.Consensus.SyncTopology()
		state := s.Consensus.Status()
		metrics.UpdateConsensusObservations(s.Consensus.Observations())
		metrics.UpdateConsensus(true, s.Consensus.IsLeader(), state["applied_index"].(uint64), state["repair_pending"].(int), state["quorum"].(int))
	} else {
		metrics.UpdateConsensus(false, false, 0, 0, 0)
	}
	s.Reg.SetSelfUsed(used)
	ids := map[string]bool{}
	for _, n := range s.Reg.Snapshot() {
		ids[n.NodeID] = true
		metrics.UpdateClusterNode(n.NodeID, n.Healthy, n.UsedBytes)
	}
	metrics.RetainClusterNodes(ids)
}

func (s *Server) handleAPIPolki(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	name := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/polki"), "/")

	if name != "" {
		s.handleAPIPolkaDetail(w, r, name)
		return
	}

	switch r.Method {
	case http.MethodGet:
		names, err := s.Store.ListPolki()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		polki := make([]storage.PolkaStats, 0, len(names))
		for _, n := range visibleShelves(r, names) {
			st, err := s.Store.StatsForPolka(n)
			if err != nil {
				st = storage.PolkaStats{Name: n}
			}
			polki = append(polki, st)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"polki": polki})
	case http.MethodPost:
		var body struct {
			Name string `json:"name"`
		}
		payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if err != nil || verifyPayload(r, payload) != nil {
			http.Error(w, "invalid or oversized request body", http.StatusBadRequest)
			return
		}
		if err := json.NewDecoder(strings.NewReader(string(payload))).Decode(&body); err != nil || strings.TrimSpace(body.Name) == "" {
			http.Error(w, "нужно поле name", http.StatusBadRequest)
			return
		}
		if err := s.createPolka(r.Context(), body.Name); err != nil {
			if errors.Is(err, errClusterChange) {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
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

func (s *Server) handleAPIPolkaDetail(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method == http.MethodDelete {
		if err := s.deletePolka(r.Context(), name); err != nil {
			status, _, _ := mapStorageErr(err)
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	st, err := s.Store.StatsForPolka(name)
	if err != nil {
		if errors.Is(err, storage.ErrPolkaNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	after := r.URL.Query().Get("start-after")
	objs, truncated, err := s.Store.ListObjects(name, "", 100, after)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"polka":     st,
		"objects":   objs,
		"truncated": truncated,
		"next_start_after": func() string {
			if truncated && len(objs) > 0 {
				return objs[len(objs)-1].Key
			}
			return ""
		}(),
	})
}

func (s *Server) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.refreshMetrics()
	names, err := s.Store.ListPolki()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	polki := make([]storage.PolkaStats, 0, len(names))
	var objects int
	var objectBytes int64
	for _, n := range names {
		st, err := s.Store.StatsForPolka(n)
		if err != nil {
			st = storage.PolkaStats{Name: n}
		}
		polki = append(polki, st)
		objects += st.Objects
		objectBytes += st.Bytes
	}
	quotaUsed, quotaTotal, err := s.Store.QuotaUsage()
	if err != nil {
		http.Error(w, "could not inspect storage quota", http.StatusInternalServerError)
		return
	}
	snap := metrics.Collect()
	if s.Consensus != nil {
		s.Consensus.SyncTopology()
	}
	nodes := s.Reg.Snapshot()
	healthy := 0
	for _, n := range nodes {
		if n.Healthy {
			healthy++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"quota":              map[string]int64{"used_bytes": quotaUsed, "total_bytes": quotaTotal},
		"consensus":          s.consensusStatus(),
		"limits":             map[string]int64{"max_object_size": s.Cfg.MaxObjectSize, "max_temp_bytes": s.Cfg.MaxTempBytes},
		"node_id":            s.Cfg.NodeID,
		"replication_factor": s.replicationFactor(),
		"nodes_total":        len(nodes),
		"nodes_healthy":      healthy,
		"nodes":              nodes,
		"polki_count":        len(polki),
		"objects_count":      objects,
		"objects_bytes":      objectBytes,
		"polki":              polki,
		"metrics":            snap,
		"terminology": map[string]string{
			"bucket": "полка",
			"store":  "кладовка",
		},
	})
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
	if s.Consensus != nil {
		s.Consensus.SyncTopology()
	}
	used, _, _ := s.Store.DiskUsage()
	s.Reg.SetSelfUsed(used)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"nodes":              s.Reg.Snapshot(),
		"consensus":          s.consensusStatus(),
		"this_node":          s.Cfg.NodeID,
		"replication_factor": s.replicationFactor(),
		"used_bytes":         used,
		"terminology": map[string]string{
			"bucket": "полка",
			"store":  "кладовка",
		},
	})
}

func (s *Server) handleInternalObject(w http.ResponseWriter, r *http.Request) {
	if s.Consensus != nil {
		http.Error(w, "legacy internal object API disabled in consensus mode", http.StatusConflict)
		return
	}
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
		body, _, err := s.spoolBody(r, false)
		if err != nil {
			s.spoolFailure(w, r, err)
			return
		}
		defer removeSpool(body)
		exists, _ := s.Store.PolkaExists(polka)
		if !exists {
			_ = s.Store.CreatePolka(polka)
		}
		meta, err := s.Store.PutObjectWithETag(polka, key, body, ct, r.Header.Get("X-Kladovka-ETag"))
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
		w.Header().Set("X-Kladovka-Sha256", meta.SHA256)
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.ServeContent(w, r, key, meta.LastModified, rc.(io.ReadSeeker))
	case http.MethodDelete:
		if err := s.Store.DeleteObject(polka, key); err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
			status, _, _ := mapStorageErr(err)
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	principal, err := s.authenticateSigned(r)
	if err == nil {
		setPrincipal(r, principal)
		if !allowedRequest(principal, r) {
			writeS3Error(w, http.StatusForbidden, "AccessDenied", "access denied", r.URL.Path)
			return false
		}
		if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
			writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "AWS streaming signatures are not supported; disable optional request checksums", r.URL.Path)
			return false
		}
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

	for _, q := range []string{"acl", "versioning", "versionId", "versions", "lifecycle", "policy", "cors", "tagging", "location", "notification", "restore"} {
		if r.URL.Query().Has(q) {
			writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "unsupported S3 feature: "+q, r.URL.Path)
			return
		}
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if r.URL.Query().Has("delete") {
		bucket, _, object := splitPolkaKey(path)
		if bucket == "" || object {
			writeS3Error(w, 400, "InvalidRequest", "delete requires a bucket endpoint", r.URL.Path)
			return
		}
		s.handleDeleteObjects(w, r, bucket)
		return
	}
	if len(r.Header.Values("X-Amz-Copy-Source")) > 0 {
		if _, _, object := splitPolkaKey(path); !object {
			writeS3Error(w, 400, "InvalidRequest", "copy destination must be an object", r.URL.Path)
			return
		}
	}
	if path == "" {
		s.handleListPolki(w, r)
		return
	}
	polka, key, hasKey := splitPolkaKey(path)
	if !hasKey {
		if r.URL.Query().Has("uploads") {
			writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "ListMultipartUploads is not supported", r.URL.Path)
			return
		}
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
	writeXML(w, http.StatusOK, listBucketsXML(visibleShelves(r, names)))
}

func (s *Server) handlePolka(w http.ResponseWriter, r *http.Request, polka string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if err := s.createPolka(r.Context(), polka); err != nil {
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
		if err := s.deletePolka(r.Context(), polka); err != nil {
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
	if len(r.Header.Values("X-Amz-Copy-Source")) > 0 {
		s.copyObject(w, r, polka, key)
		return
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) && (r.URL.Query().Has("uploads") || r.URL.Query().Has("uploadId")) {
		s.forwardToPrimary(w, r, polka, key)
		return
	}
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
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		// Forward to primary.
		s.forwardToPrimary(w, r, polka, key)
		return
	}
	body, size, err := s.spoolBody(r, true)
	if err != nil {
		s.spoolFailure(w, r, err)
		return
	}
	defer removeSpool(body)
	exists, _ := s.Store.PolkaExists(polka)
	if !exists {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified shelf (полка) does not exist", "/"+polka)
		return
	}
	ct := r.Header.Get("Content-Type")
	meta, err := s.Store.PutObject(polka, key, body, ct)
	if err != nil {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
		if err := s.Rep.ReplicatePut(r.Context(), polka, key, body, ct, meta.ETag); err != nil {
			metrics.RecordReplicationFailed()
			writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "object stored locally but replication incomplete; retry the same PUT", r.URL.Path)
			return
		} else {
			metrics.RecordReplicationOK()
			metrics.RecordReplicationPublished(s.Cfg.ReplicationFactor - 1)
		}
	}
	metrics.RecordS3Operation("PutObject", polka)
	metrics.RecordBytesUploaded(polka, size)
	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	rc, meta, err := s.Store.GetObject(polka, key)
	if err == nil {
		defer rc.Close()
		w.Header().Set("ETag", meta.ETag)
		w.Header().Set("Content-Type", meta.ContentType)
		w.Header().Set("x-amz-kladovka-polka", polka)
		counted := &responseCounter{ResponseWriter: w}
		http.ServeContent(counted, r, key, meta.LastModified, rc.(io.ReadSeeker))
		if counted.status == http.StatusOK || counted.status == http.StatusPartialContent {
			metrics.RecordS3Operation("GetObject", polka)
			metrics.RecordBytesDownloaded(polka, counted.bytes)
		}
		return
	}
	if !errors.Is(err, storage.ErrObjectNotFound) {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	// Try peers if not found locally.
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
		if s.tryFetchFromPeers(w, r, polka, key) {
			return
		}
	}
	writeS3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist", r.URL.Path)
}

func (s *Server) headObject(w http.ResponseWriter, r *http.Request, polka, key string) {
	meta, err := s.Store.HeadObject(polka, key)
	if err != nil {
		if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && s.tryFetchFromPeers(w, r, polka, key) {
			return
		}
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
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		s.forwardToPrimary(w, r, polka, key)
		return
	}
	err := s.Store.DeleteObject(polka, key)
	if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
		st, code, msg := mapStorageErr(err)
		writeS3Error(w, st, code, msg, r.URL.Path)
		return
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
		if err := s.Rep.ReplicateDelete(r.Context(), polka, key); err != nil {
			metrics.RecordReplicationFailed()
			writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "local delete completed; replica delete incomplete, retry DELETE", r.URL.Path)
			return
		}
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
	if strings.HasPrefix(r.URL.Path, "/api/object/") {
		u = strings.TrimRight(base, "/") + r.URL.EscapedPath()
	}
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, u, r.Body)
	if err == nil {
		req.ContentLength = r.ContentLength
	}
	if err != nil {
		writeS3Error(w, http.StatusBadGateway, "InternalError", err.Error(), r.URL.Path)
		return
	}
	for k, vv := range r.Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host
	if active, _ := r.Context().Value(sessionContextKey{}).(bool); active {
		// Browser sessions are node-local; delegate an authenticated admin operation.
		req.Header.Del("Cookie")
		req.Header.Del("X-CSRF-Token")
		user, password := s.adminCredentials()
		req.SetBasicAuth(user, password)
	}
	resp, err := s.Reg.Client().Do(req)
	if err != nil {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", err.Error(), r.URL.Path)
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
		req, err := http.NewRequestWithContext(r.Context(), r.Method, u, nil)
		if err != nil {
			continue
		}
		for _, name := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
			if v := r.Header.Get(name); v != "" {
				req.Header.Set(name, v)
			}
		}
		s.Reg.Authorize(req)
		resp, err := s.Reg.Client().Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusNotModified {
			_ = resp.Body.Close()
			continue
		}
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		n, _ := io.Copy(w, resp.Body)
		if r.Method == http.MethodGet {
			metrics.RecordS3Operation("GetObject", polka)
			metrics.RecordBytesDownloaded(polka, n)
		} else if r.Method == http.MethodHead {
			metrics.RecordS3Operation("HeadObject", polka)
		}
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
	// Callers pass URL.Path, which net/http has already decoded once.
	// Decoding again aliases literal %2F/%25 sequences and drops invalid escapes.
	key = path[i+1:]
	return polka, key, true
}

func escapeKeyPath(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func (s *Server) handleAPIObject(w http.ResponseWriter, r *http.Request) {
	polka, key, ok := splitPolkaKey(strings.TrimPrefix(r.URL.Path, "/api/object/"))
	if !ok {
		http.Error(w, "missing object key", http.StatusBadRequest)
		return
	}
	s.handleObject(w, r, polka, key)
}
