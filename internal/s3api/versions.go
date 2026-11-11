// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/storage"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func setVersionHeaders(w http.ResponseWriter, m *storage.ObjectMeta) {
	if m == nil {
		return
	}
	if m.VersionID != "" {
		w.Header().Set("X-Amz-Version-Id", m.VersionID)
	}
	if m.DeleteMarker {
		w.Header().Set("X-Amz-Delete-Marker", "true")
	}
}
func (s *Server) versions() storage.VersionBackend {
	v, _ := s.Store.(storage.VersionBackend)
	return v
}
func (s *Server) trash(polka, key string) (*storage.ObjectMeta, error) {
	if s.Cfg.VersioningEnabled && s.versions() != nil {
		return s.versions().TrashObject(polka, key)
	}
	return nil, s.Store.DeleteObject(polka, key)
}
func (s *Server) putInternalVersion(r *http.Request, polka, key string, body io.Reader, ct string) (*storage.ObjectMeta, error) {
	if raw := r.Header.Get("X-Kladovka-Version-Meta"); raw != "" {
		var m storage.ObjectMeta
		if json.Unmarshal([]byte(raw), &m) != nil || m.Key != key || m.DeleteMarker || !s.Cfg.VersioningEnabled {
			return nil, storage.ErrIntegrity
		}
		if v, ok := s.Store.(*storage.Store); ok {
			return v.PutReplica(polka, key, body, m)
		}
	}
	if s.Cfg.VersioningEnabled {
		return nil, errors.New("version metadata required; upgrade all replicas")
	}
	return s.Store.PutObjectWithETag(polka, key, body, ct, r.Header.Get("X-Kladovka-ETag"))
}
func (s *Server) deleteInternalVersion(r *http.Request, polka, key string) error {
	if s.Cfg.VersioningEnabled && s.versions() != nil {
		if id := r.URL.Query().Get("versionId"); id != "" {
			return s.versions().DeleteVersion(polka, key, id)
		}
		if id := r.URL.Query().Get("expire"); id != "" {
			return s.versions().ExpireTrash(polka, key, id)
		}
		var m storage.ObjectMeta
		if json.Unmarshal([]byte(r.Header.Get("X-Kladovka-Version-Meta")), &m) != nil || !m.DeleteMarker || m.Key != key {
			return storage.ErrIntegrity
		}
		if v, ok := s.Store.(*storage.Store); ok {
			return v.DeleteReplica(polka, key, m)
		}
	}
	return s.Store.DeleteObject(polka, key)
}
func (s *Server) versionError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, msg := mapStorageErr(err)
	writeS3Error(w, status, code, msg, r.URL.Path)
}
func (s *Server) handleVersionRequest(w http.ResponseWriter, r *http.Request, path string) bool {
	q := r.URL.Query()
	if !q.Has("versionId") && !q.Has("versions") && !q.Has("versioning") {
		return false
	}
	if !s.Cfg.VersioningEnabled || s.versions() == nil {
		writeS3Error(w, 501, "NotImplemented", "versioning disabled", r.URL.Path)
		return true
	}
	polka, key, object := splitPolkaKey(path)
	if q.Has("versioning") && !object && r.Method == "GET" {
		if exists, err := s.Store.PolkaExists(polka); err != nil {
			s.versionError(w, r, err)
			return true
		} else if !exists {
			s.versionError(w, r, storage.ErrPolkaNotFound)
			return true
		}
		writeXML(w, 200, struct {
			XMLName xml.Name `xml:"VersioningConfiguration"`
			Xmlns   string   `xml:"xmlns,attr"`
			Status  string   `xml:"Status"`
		}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Status: "Enabled"})
		return true
	}
	if q.Has("versions") && !object && r.Method == "GET" {
		s.listVersionsXML(w, r, polka)
		return true
	}
	if !object || !q.Has("versionId") || q.Get("versionId") == "" {
		writeS3Error(w, 400, "InvalidRequest", "invalid version operation", r.URL.Path)
		return true
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		s.forwardToPrimary(w, r, polka, key)
		return true
	}
	id := q.Get("versionId")
	switch r.Method {
	case "GET", "HEAD":
		body, m, err := s.versions().GetVersion(polka, key, id)
		setVersionHeaders(w, m)
		if err != nil {
			if m != nil && m.DeleteMarker {
				writeS3Error(w, 405, "MethodNotAllowed", "requested version is a delete marker", r.URL.Path)
			} else {
				s.versionError(w, r, err)
			}
			return true
		}
		defer body.Close()
		w.Header().Set("ETag", m.ETag)
		w.Header().Set("Content-Type", m.ContentType)
		http.ServeContent(w, r, key, m.LastModified, body.(io.ReadSeeker))
	case "DELETE":
		p, ok := requestPrincipal(r)
		if !ok || p.Role != "admin" {
			writeS3Error(w, 403, "AccessDenied", "permanent version deletion requires admin", r.URL.Path)
			return true
		}
		err := s.versions().DeleteVersion(polka, key, id)
		if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
			s.versionError(w, r, err)
			return true
		}
		if err = s.replicateHistory(r.Context(), polka, key, "versionId", id); err != nil {
			writeS3Error(w, 503, "ServiceUnavailable", "version deletion replication incomplete", r.URL.Path)
			return true
		}
		w.Header().Set("X-Amz-Version-Id", id)
		w.WriteHeader(204)
	default:
		writeS3Error(w, 405, "MethodNotAllowed", "unsupported version method", r.URL.Path)
	}
	return true
}

type xmlVersion struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
	Latest    bool   `xml:"IsLatest"`
	Modified  string `xml:"LastModified"`
	ETag      string `xml:"ETag,omitempty"`
	Size      *int64 `xml:"Size,omitempty"`
}

func (s *Server) listVersionsXML(w http.ResponseWriter, r *http.Request, polka string) {
	q := r.URL.Query()
	if q.Get("delimiter") != "" || (q.Get("encoding-type") != "" && q.Get("encoding-type") != "url") {
		writeS3Error(w, 501, "NotImplemented", "version listing supports encoding-type=url without delimiter", r.URL.Path)
		return
	}
	limit := 1000
	if q.Has("max-keys") {
		n, e := strconv.Atoi(q.Get("max-keys"))
		if e != nil || n < 0 || n > 1000 {
			writeS3Error(w, 400, "InvalidArgument", "invalid max-keys", r.URL.Path)
			return
		}
		limit = n
	}
	all, err := s.versions().ListVersions(polka, q.Get("prefix"))
	if err != nil {
		s.versionError(w, r, err)
		return
	}
	start := 0
	marker := q.Get("key-marker")
	vid := q.Get("version-id-marker")
	if vid != "" && marker == "" {
		writeS3Error(w, 400, "InvalidArgument", "version marker requires key marker", r.URL.Path)
		return
	}
	if marker != "" {
		for start < len(all) && all[start].Key < marker {
			start++
		}
		if vid == "" {
			for start < len(all) && all[start].Key == marker {
				start++
			}
		} else {
			found := false
			for start < len(all) && all[start].Key == marker {
				if all[start].VersionID == vid {
					start++
					found = true
					break
				}
				start++
			}
			if !found {
				writeS3Error(w, 400, "InvalidArgument", "unknown version marker", r.URL.Path)
				return
			}
		}
	}
	result := struct {
		XMLName       xml.Name     `xml:"ListVersionsResult"`
		EncodingType  string       `xml:"EncodingType,omitempty"`
		Xmlns         string       `xml:"xmlns,attr"`
		Name          string       `xml:"Name"`
		Prefix        string       `xml:"Prefix"`
		MaxKeys       int          `xml:"MaxKeys"`
		Truncated     bool         `xml:"IsTruncated"`
		KeyMarker     string       `xml:"KeyMarker"`
		VersionMarker string       `xml:"VersionIdMarker"`
		NextKey       string       `xml:"NextKeyMarker,omitempty"`
		NextVersion   string       `xml:"NextVersionIdMarker,omitempty"`
		Versions      []xmlVersion `xml:"Version"`
		Markers       []xmlVersion `xml:"DeleteMarker"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: polka, Prefix: q.Get("prefix"), MaxKeys: limit, KeyMarker: marker, VersionMarker: vid}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	if limit > 0 && end < len(all) {
		result.Truncated = true
		result.NextKey = all[end-1].Key
		result.NextVersion = all[end-1].VersionID
	}
	for _, v := range all[start:end] {
		x := xmlVersion{Key: v.Key, VersionID: v.VersionID, Latest: v.IsLatest, Modified: v.LastModified.UTC().Format(time.RFC3339Nano)}
		if v.DeleteMarker {
			result.Markers = append(result.Markers, x)
		} else {
			x.ETag = v.ETag
			size := v.Size
			x.Size = &size
			result.Versions = append(result.Versions, x)
		}
	}
	if q.Get("encoding-type") == "url" {
		result.EncodingType = "url"
		encode := func(v string) string { return strings.ReplaceAll(url.QueryEscape(v), "+", "%20") }
		result.Prefix = encode(result.Prefix)
		result.KeyMarker = encode(result.KeyMarker)
		result.NextKey = encode(result.NextKey)
		for i := range result.Versions {
			result.Versions[i].Key = encode(result.Versions[i].Key)
		}
		for i := range result.Markers {
			result.Markers[i].Key = encode(result.Markers[i].Key)
		}
	}
	writeXML(w, 200, result)
}
func (s *Server) replicateHistory(ctx context.Context, polka, key, param, id string) error {
	if s.Consensus != nil || s.Cfg.ReplicationFactor <= 1 {
		return nil
	}
	for _, node := range s.Rep.Placement(polka, key) {
		if node == s.Reg.SelfID() {
			continue
		}
		base := s.Reg.GetURL(node)
		if base == "" {
			return errors.New("replica unavailable")
		}
		req, err := http.NewRequestWithContext(ctx, "DELETE", strings.TrimRight(base, "/")+"/internal/object/"+url.PathEscape(polka)+"/"+escapeKeyPath(key)+"?"+param+"="+url.QueryEscape(id), nil)
		if err != nil {
			return err
		}
		s.Reg.Authorize(req)
		resp, err := s.Reg.Client().Do(req)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 && resp.StatusCode != 404 {
			return errors.New("replica history mutation failed")
		}
	}
	return nil
}
func (s *Server) handleAPIVersions(w http.ResponseWriter, r *http.Request) {
	if !s.Cfg.VersioningEnabled || s.versions() == nil {
		http.Error(w, "versioning disabled", 501)
		return
	}
	polka, key, object := splitPolkaKey(strings.TrimPrefix(r.URL.Path, "/api/versions/"))
	if object && s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		s.forwardToPrimary(w, r, polka, key)
		return
	}
	if r.Method == "GET" {
		all, err := s.versions().ListVersions(polka, r.URL.Query().Get("prefix"))
		if err != nil {
			s.versionError(w, r, err)
			return
		}
		if object {
			filtered := all[:0]
			for _, v := range all {
				if v.Key == key {
					filtered = append(filtered, v)
				}
			}
			all = filtered
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"versions": all, "trash_retention_sec": int64(s.Cfg.TrashRetention / time.Second)})
		return
	}
	if !object || r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, key) {
		s.forwardToPrimary(w, r, polka, key)
		return
	}
	var request struct {
		VersionID string `json:"version_id"`
		Action    string `json:"action"`
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil || verifyPayload(r, payload) != nil || json.Unmarshal(payload, &request) != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	switch request.Action {
	case "restore":
		body, m, err := s.versions().GetVersion(polka, key, request.VersionID)
		if err != nil {
			s.versionError(w, r, err)
			return
		}
		defer body.Close()
		if s.Cfg.MaxObjectSize > 0 && m.Size > s.Cfg.MaxObjectSize {
			http.Error(w, "object exceeds configured limit", 413)
			return
		}
		result, err := s.Store.PutObjectWithETag(polka, key, body, m.ContentType, m.ETag)
		if err != nil {
			s.versionError(w, r, err)
			return
		}
		if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
			err = s.Rep.ReplicatePut(r.Context(), polka, key, body.(io.ReadSeeker), result.ContentType, result.ETag, result)
		}
		if err != nil {
			http.Error(w, "restore replication incomplete", 503)
			return
		}
		setVersionHeaders(w, result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	case "purge":
		if err = s.versions().ExpireTrash(polka, key, request.VersionID); err == nil {
			err = s.replicateHistory(r.Context(), polka, key, "expire", request.VersionID)
		}
		if err != nil {
			s.versionError(w, r, err)
			return
		}
		w.WriteHeader(204)
	default:
		http.Error(w, "unknown action", 400)
	}
}
func (s *Server) CleanupTrash(ctx context.Context, now time.Time) error {
	if !s.Cfg.VersioningEnabled || s.Cfg.TrashRetention == 0 || s.versions() == nil {
		return nil
	}
	if s.Consensus != nil && !s.Consensus.IsLeader() {
		return nil
	}
	polki, err := s.Store.ListPolki()
	if err != nil {
		return err
	}
	for _, polka := range polki {
		all, err := s.versions().ListVersions(polka, "")
		if err != nil {
			return err
		}
		for _, v := range all {
			if !v.IsLatest || !v.DeleteMarker || now.Before(v.LastModified.Add(s.Cfg.TrashRetention)) {
				continue
			}
			if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(polka, v.Key) {
				continue
			}
			s.historyMu.Lock()
			err = s.expireAudited(ctx, polka, v.Key, v.VersionID)
			s.historyMu.Unlock()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) expireAudited(ctx context.Context, polka, key, id string) error {
	event := audit.Event{Node: s.Cfg.NodeID, ID: randomToken(), Kind: "attempt", Actor: "trash_cleanup", Method: "DELETE", Path: "/" + polka + "/" + key + "?trash_generation=" + id}
	if s.Audit != nil {
		if err := s.Audit.Append(event); err != nil {
			return err
		}
	}
	err := s.replicateHistory(ctx, polka, key, "expire", id)
	if err == nil {
		err = s.versions().ExpireTrash(polka, key, id)
	}
	event.Kind = "result"
	event.Status = 204
	if err != nil {
		event.Status = 503
	}
	if s.Audit != nil {
		if auditErr := s.Audit.Append(event); auditErr != nil {
			return auditErr
		}
	}
	return err
}

// Serialize legacy primary mutations through their replication acknowledgement.
// Internal replica writes use the storage lock and must not take this lock.
func (s *Server) serializeHistory(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if s.Cfg.VersioningEnabled && s.Consensus == nil && r.Method != "GET" && r.Method != "HEAD" && !strings.HasPrefix(path, "/internal/") {
			path = strings.TrimPrefix(strings.TrimPrefix(path, "/api/object/"), "/api/versions/")
			polka, key, ok := splitPolkaKey(path)
			if ok && (s.Cfg.ReplicationFactor <= 1 || s.Rep.IsPrimary(polka, key)) {
				s.historyMu.Lock()
				defer s.historyMu.Unlock()
			}
		}
		next.ServeHTTP(w, r)
	})
}
