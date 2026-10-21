// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rkislov/kladovka/internal/metrics"
	"github.com/rkislov/kladovka/internal/storage"
)

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	fail := func(status int, code, message string) { writeS3Error(w, status, code, message, r.URL.Path) }
	if r.Method != http.MethodPut {
		fail(405, "MethodNotAllowed", "copy requires PUT")
		return
	}
	if r.URL.Query().Has("uploadId") || r.URL.Query().Has("uploads") || r.URL.Query().Has("partNumber") {
		fail(501, "NotImplemented", "UploadPartCopy is not supported")
		return
	}
	raw := r.Header.Get("X-Amz-Copy-Source")
	if len(r.Header.Values("X-Amz-Copy-Source")) != 1 {
		fail(400, "InvalidArgument", "one copy source required")
		return
	}
	versionID := ""
	if path, query, ok := strings.Cut(raw, "?"); ok {
		values, e := url.ParseQuery(query)
		if e != nil || len(values) != 1 || values.Get("versionId") == "" || !s.Cfg.VersioningEnabled {
			fail(501, "NotImplemented", "unsupported copy source query")
			return
		}
		raw = path
		versionID = values.Get("versionId")
	}

	source, err := url.PathUnescape(strings.TrimPrefix(raw, "/"))
	srcBucket, srcKey, ok := strings.Cut(source, "/")
	if err != nil || !ok || srcBucket == "" || srcKey == "" || strings.ContainsAny(srcBucket, `\:`) || strings.Contains(srcBucket, "..") || strings.HasPrefix(srcBucket, ".") {
		fail(400, "InvalidArgument", "copy source must be an encoded bucket/key")
		return
	}
	p, authenticated := requestPrincipal(r)
	if !authenticated || !p.CanRead(srcBucket) || !p.CanWrite(bucket) {
		fail(403, "AccessDenied", "copy requires source read and destination write access")
		return
	}
	directive := r.Header.Get("X-Amz-Metadata-Directive")
	if directive == "" {
		directive = "COPY"
	}
	if directive != "COPY" && directive != "REPLACE" {
		fail(400, "InvalidArgument", "metadata directive must be COPY or REPLACE")
		return
	}
	// Never silently accept requested ACL, encryption, metadata, tagging or
	// destination conditions which the backend does not implement.
	allowed := map[string]bool{"x-amz-copy-source": true, "x-amz-copy-source-if-match": true, "x-amz-copy-source-if-none-match": true, "x-amz-copy-source-if-modified-since": true, "x-amz-copy-source-if-unmodified-since": true, "x-amz-metadata-directive": true, "x-amz-tagging-directive": true, "x-amz-storage-class": true, "x-amz-content-sha256": true, "x-amz-date": true}
	for h := range r.Header {
		name := strings.ToLower(h)
		if strings.HasPrefix(name, "x-amz-") && !allowed[name] {
			fail(501, "NotImplemented", "unsupported copy header: "+h)
			return
		}
	}
	for _, h := range []string{"If-Match", "If-None-Match", "Cache-Control", "Content-Encoding", "Content-Disposition", "Content-Language", "Expires"} {
		if r.Header.Get(h) != "" {
			fail(501, "NotImplemented", "unsupported copy header: "+h)
			return
		}
	}
	if v := r.Header.Get("X-Amz-Tagging-Directive"); v != "" && v != "COPY" {
		fail(501, "NotImplemented", "tagging is not supported")
		return
	}
	if v := r.Header.Get("X-Amz-Storage-Class"); v != "" && v != "STANDARD" {
		fail(501, "NotImplemented", "storage class is not supported")
		return
	}
	// Copy semantics must be covered by the authenticated signature. Basic/session
	// requests are permitted only by the existing JSON API authentication layer.
	signed := r.URL.Query().Get("X-Amz-SignedHeaders")
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		for _, field := range strings.Split(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "), ",") {
			if v, ok := strings.CutPrefix(strings.TrimSpace(field), "SignedHeaders="); ok {
				signed = v
			}
		}
	}
	if signed != "" {
		for h := range r.Header {
			name := strings.ToLower(h)
			if (strings.HasPrefix(name, "x-amz-") || (name == "content-type" && directive == "REPLACE")) && !strings.Contains(";"+signed+";", ";"+name+";") {
				// SigV4 permits the payload hash to be authenticated via the canonical payload.
				if name == "x-amz-content-sha256" {
					continue
				}
				fail(403, "AccessDenied", "copy headers must be signed")
				return
			}
		}
	}
	if srcBucket == bucket && srcKey == key && directive == "COPY" && versionID == "" {
		fail(400, "InvalidRequest", "self copy requires REPLACE")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) != 0 || verifyPayload(r, body) != nil {
		fail(400, "InvalidRequest", "copy requires an empty verified request body")
		return
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(bucket, key) {
		s.forwardToPrimary(w, r, bucket, key)
		return
	}
	exists, err := s.Store.PolkaExists(bucket)
	if err != nil {
		st, c, m := mapStorageErr(err)
		fail(st, c, m)
		return
	}
	if !exists {
		fail(404, "NoSuchBucket", "destination bucket does not exist")
		return
	}
	reader, meta, err := s.copySourceVersion(r, srcBucket, srcKey, versionID)
	if err != nil {
		st, c, m := mapStorageErr(err)
		fail(st, c, m)
		return
	}
	defer reader.Close()
	if meta.Size > 5<<30 {
		fail(413, "EntityTooLarge", "CopyObject supports at most 5 GiB")
		return
	}
	if status := copyConditions(r, meta); status != 0 {
		if status == 400 {
			fail(status, "InvalidArgument", "invalid copy condition date")
		} else {
			fail(status, "PreconditionFailed", "copy source condition failed")
		}
		return
	}
	// Pin/read the source generation before publishing the destination. The spool
	// uses the same size, temporary-space, free-space and concurrency limits as PUT.
	transfer := r.Clone(r.Context())
	transfer.Body = reader
	transfer.ContentLength = meta.Size
	spool, n, err := s.spoolBody(transfer, false)
	if err != nil {
		s.spoolFailure(w, r, err)
		return
	}
	defer removeSpool(spool)
	if n != meta.Size {
		fail(503, "ServiceUnavailable", "copy source length changed")
		return
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, spool); err != nil {
		s.spoolFailure(w, r, err)
		return
	}
	if len(meta.SHA256) != 64 || !secureEqual(hex.EncodeToString(hash.Sum(nil)), meta.SHA256) {
		fail(500, "InvalidObjectState", "copy source integrity check failed")
		return
	}
	if _, err = spool.Seek(0, io.SeekStart); err != nil {
		s.spoolFailure(w, r, err)
		return
	}
	ct := meta.ContentType
	if directive == "REPLACE" {
		ct = r.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}
	}
	result, err := s.Store.PutObject(bucket, key, spool, ct)
	if err != nil {
		st, c, m := mapStorageErr(err)
		fail(st, c, m)
		return
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
		if err = s.Rep.ReplicatePut(r.Context(), bucket, key, spool, ct, result.ETag, result); err != nil {
			metrics.RecordReplicationFailed()
			fail(503, "ServiceUnavailable", "copy stored locally but replication incomplete; retry copy")
			return
		}
		metrics.RecordReplicationOK()
		metrics.RecordReplicationPublished(s.Cfg.ReplicationFactor - 1)
	}
	setVersionHeaders(w, result)
	metrics.RecordS3Operation("CopyObject", bucket)
	writeXML(w, 200, struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		Xmlns        string   `xml:"xmlns,attr"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
	}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", LastModified: s3Time(result.LastModified), ETag: result.ETag})
}

func copyConditions(r *http.Request, meta *storage.ObjectMeta) int {
	match, none := r.Header.Get("X-Amz-Copy-Source-If-Match"), r.Header.Get("X-Amz-Copy-Source-If-None-Match")
	etag := strings.Trim(meta.ETag, "\"")
	if match != "" && strings.Trim(match, "\"") != etag && match != "*" {
		return 412
	}
	if none != "" && (strings.Trim(none, "\"") == etag || none == "*") {
		return 412
	}
	for _, item := range []struct {
		header   string
		skip     bool
		modified bool
	}{
		{"X-Amz-Copy-Source-If-Unmodified-Since", match != "", false},
		{"X-Amz-Copy-Source-If-Modified-Since", none != "", true},
	} {
		if value := r.Header.Get(item.header); value != "" {
			stamp, err := http.ParseTime(value)
			if err != nil {
				return 400
			}
			if !item.skip && meta.LastModified.Truncate(time.Second).After(stamp) != item.modified {
				return 412
			}
		}
	}
	return 0
}

func (s *Server) copySource(r *http.Request, bucket, key string) (io.ReadCloser, *storage.ObjectMeta, error) {
	return s.copySourceVersion(r, bucket, key, "")
}
func (s *Server) copySourceVersion(r *http.Request, bucket, key, id string) (io.ReadCloser, *storage.ObjectMeta, error) {
	if s.Consensus != nil || s.Cfg.ReplicationFactor <= 1 || s.Rep.IsPrimary(bucket, key) {
		if id != "" {
			return s.versions().GetVersion(bucket, key, id)
		}
		return s.Store.GetObject(bucket, key)
	}
	// Source placement can differ from destination placement. Fetch only the
	// registered source primary, never a caller-controlled URL or a stale fallback.
	base := s.Rep.PrimaryURL(bucket, key)
	if base == "" {
		return nil, nil, errClusterChange
	}
	req, err := http.NewRequestWithContext(r.Context(), "GET", strings.TrimRight(base, "/")+"/internal/object/"+url.PathEscape(bucket)+"/"+escapeKeyPath(key), nil)
	if err != nil {
		return nil, nil, err
	}
	if id != "" {
		req.URL.RawQuery = "versionId=" + url.QueryEscape(id)
	}
	req.Header.Set("Authorization", "Bearer "+s.Cfg.ClusterToken)
	client := *s.Reg.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return nil, nil, errClusterChange
	}
	if response.StatusCode != 200 {
		response.Body.Close()
		if response.StatusCode == 404 {
			return nil, nil, storage.ErrObjectNotFound
		}
		return nil, nil, errClusterChange
	}
	stamp, err := http.ParseTime(response.Header.Get("Last-Modified"))
	if err != nil || response.ContentLength < 0 || response.Header.Get("ETag") == "" {
		response.Body.Close()
		return nil, nil, errors.New("invalid copy source response")
	}
	return response.Body, &storage.ObjectMeta{Key: key, Size: response.ContentLength, ETag: response.Header.Get("ETag"), SHA256: response.Header.Get("X-Kladovka-Sha256"), ContentType: response.Header.Get("Content-Type"), LastModified: stamp}, nil
}
