// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"hash/crc32"
	"hash/crc64"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/metrics"
	"github.com/rkislov/kladovka/internal/storage"
)

const maxDeleteBody = 2 << 20

var errDeleteXML = errors.New("invalid Delete XML")
var errDeleteFeature = errors.New("versioned or conditional deletion is not supported")

type deleteBatch struct {
	Keys  []string
	Quiet bool
}
type deletedKey struct {
	Key string `xml:"Key"`
}
type deleteFailure struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}
type deleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []deletedKey    `xml:"Deleted"`
	Errors  []deleteFailure `xml:"Error"`
}

// Parse the entire bounded document before performing any mutation. Unknown,
// duplicate or nested fields must never turn a conditional delete into a delete.
func parseDeleteBatch(body []byte) (deleteBatch, error) {
	var result deleteBatch
	decoder := xml.NewDecoder(bytes.NewReader(body))
	stack := []string{}
	rootSeen, rootDone, quietSeen, keySeen := false, false, false, false
	var value strings.Builder
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, errDeleteXML
		}
		switch item := token.(type) {
		case xml.StartElement:
			if rootDone || (item.Name.Space != "" && item.Name.Space != "http://s3.amazonaws.com/doc/2006-03-01/") {
				return result, errDeleteXML
			}
			for _, attr := range item.Attr {
				if !(len(stack) == 0 && (attr.Name.Local == "xmlns" || attr.Name.Space == "xmlns")) {
					return result, errDeleteXML
				}
			}
			name := item.Name.Local
			switch len(stack) {
			case 0:
				if rootSeen || name != "Delete" {
					return result, errDeleteXML
				}
				rootSeen = true
			case 1:
				if name == "Object" {
					keySeen = false
					if len(result.Keys) >= 1000 {
						return result, errDeleteXML
					}
				} else if name == "Quiet" && !quietSeen {
					quietSeen = true
					value.Reset()
				} else {
					return result, errDeleteXML
				}
			case 2:
				if stack[1] != "Object" {
					return result, errDeleteXML
				}
				if name == "VersionId" || name == "ETag" || name == "Size" || name == "LastModifiedTime" {
					return result, errDeleteFeature
				}
				if name != "Key" || keySeen {
					return result, errDeleteXML
				}
				keySeen = true
				value.Reset()
			default:
				return result, errDeleteXML
			}
			stack = append(stack, name)
		case xml.EndElement:
			if len(stack) == 0 {
				return result, errDeleteXML
			}
			switch stack[len(stack)-1] {
			case "Key":
				result.Keys = append(result.Keys, value.String())
			case "Quiet":
				switch strings.TrimSpace(value.String()) {
				case "true", "1":
					result.Quiet = true
				case "false", "0":
				default:
					return result, errDeleteXML
				}
			case "Object":
				if !keySeen {
					return result, errDeleteXML
				}
			case "Delete":
				rootDone = true
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 && (stack[len(stack)-1] == "Key" || stack[len(stack)-1] == "Quiet") {
				value.Write(item)
			} else if strings.TrimSpace(string(item)) != "" {
				return result, errDeleteXML
			}
		case xml.Directive:
			return result, errDeleteXML
		case xml.ProcInst:
			if rootSeen || item.Target != "xml" {
				return result, errDeleteXML
			}
		}
	}
	if !rootDone || len(stack) != 0 || len(result.Keys) == 0 {
		return result, errDeleteXML
	}
	return result, nil
}

var crc64NVMETable = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func deleteChecksum(r *http.Request, body []byte) (int, string) {
	md := md5.Sum(body)
	sha := sha256.Sum256(body)
	one := sha1.Sum(body)
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(body))
	cast := make([]byte, 4)
	binary.BigEndian.PutUint32(cast, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	nvme := make([]byte, 8)
	binary.BigEndian.PutUint64(nvme, crc64.Checksum(body, crc64NVMETable))
	sums := map[string][]byte{"X-Amz-Checksum-Crc64nvme": nvme, "Content-Md5": md[:], "X-Amz-Checksum-Crc32": crc, "X-Amz-Checksum-Crc32c": cast, "X-Amz-Checksum-Sha1": one[:], "X-Amz-Checksum-Sha256": sha[:]}
	count := 0
	for header, sum := range sums {
		if values := r.Header.Values(header); len(values) > 0 {
			count++
			if len(values) != 1 || values[0] != base64.StdEncoding.EncodeToString(sum) {
				return 400, "BadDigest"
			}
		}
	}
	if count == 0 {
		return 400, "MissingContentMD5"
	}
	if algorithm := r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"); algorithm != "" {
		header := "X-Amz-Checksum-" + algorithm
		if algorithm == "MD5" {
			header = "Content-MD5"
		}
		if r.Header.Get(header) == "" {
			return 400, "BadDigest"
		}
	}
	if verifyPayload(r, body) != nil {
		return 400, "XAmzContentSHA256Mismatch"
	}
	// With an unsigned payload, a checksum must itself be signed so a caller
	// cannot substitute both the XML and its checksum on a presigned request.
	if r.Header.Get("X-Amz-Content-Sha256") == "" || r.Header.Get("X-Amz-Content-Sha256") == "UNSIGNED-PAYLOAD" || r.URL.Query().Has("X-Amz-Algorithm") {
		signed := r.URL.Query().Get("X-Amz-SignedHeaders")
		for _, field := range strings.Split(r.Header.Get("Authorization"), ",") {
			if v, ok := strings.CutPrefix(strings.TrimSpace(field), "SignedHeaders="); ok {
				signed = v
			}
		}
		protected := false
		for header := range sums {
			if r.Header.Get(header) != "" && strings.Contains(";"+signed+";", ";"+strings.ToLower(header)+";") {
				protected = true
			}
		}
		if !protected {
			return 403, "AccessDenied"
		}
	}
	return 0, ""
}

func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	fail := func(status int, code, message string) { writeS3Error(w, status, code, message, r.URL.Path) }
	if r.Method != "POST" {
		fail(405, "MethodNotAllowed", "DeleteObjects requires POST")
		return
	}
	for _, query := range []string{"uploads", "uploadId", "partNumber"} {
		if r.URL.Query().Has(query) {
			fail(400, "InvalidRequest", "delete cannot be combined with multipart parameters")
			return
		}
	}
	values := r.URL.Query()["delete"]
	if len(values) != 1 || values[0] != "" {
		fail(400, "InvalidRequest", "one empty delete subresource required")
		return
	}
	p, ok := requestPrincipal(r)
	if !ok || !p.CanWrite(bucket) {
		fail(403, "AccessDenied", "bucket write access required")
		return
	}
	allowed := map[string]bool{"x-amz-content-sha256": true, "x-amz-date": true, "x-amz-sdk-checksum-algorithm": true, "x-amz-checksum-crc64nvme": true, "x-amz-checksum-crc32": true, "x-amz-checksum-crc32c": true, "x-amz-checksum-sha1": true, "x-amz-checksum-sha256": true}
	for header := range r.Header {
		name := strings.ToLower(header)
		if strings.HasPrefix(name, "x-amz-") && !allowed[name] {
			fail(501, "NotImplemented", "unsupported delete header: "+header)
			return
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDeleteBody))
	if err != nil {
		fail(400, "MaxMessageLengthExceeded", "delete XML exceeds 2 MiB or cannot be read")
		return
	}
	if status, code := deleteChecksum(r, body); status != 0 {
		fail(status, code, "delete body checksum verification failed")
		return
	}
	batch, err := parseDeleteBatch(body)
	if err != nil {
		if errors.Is(err, errDeleteFeature) {
			fail(501, "NotImplemented", err.Error())
		} else {
			fail(400, "MalformedXML", "expected Delete with 1–1000 Object/Key entries and optional Quiet")
		}
		return
	}
	exists, err := s.Store.PolkaExists(bucket)
	if err != nil {
		status, code, msg := mapStorageErr(err)
		fail(status, code, msg)
		return
	}
	if !exists {
		fail(404, "NoSuchBucket", "bucket does not exist")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	result := deleteResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	completed := map[string]*deleteFailure{}
	for _, key := range batch.Keys {
		failure, seen := completed[key]
		if !seen {
			failure = s.auditedBatchDelete(ctx, p.ID, bucket, key)
			completed[key] = failure
		}
		if failure != nil {
			result.Errors = append(result.Errors, *failure)
		} else if !batch.Quiet {
			result.Deleted = append(result.Deleted, deletedKey{key})
		}
	}
	metrics.RecordS3Operation("DeleteObjects", bucket)
	writeXML(w, 200, result)
}

func validDeleteKey(key string) bool {
	if key == "" || len(key) > 1024 || strings.Contains(key, "..") || strings.ContainsRune(key, 0) || strings.HasSuffix(key, ".tmp") || strings.HasSuffix(key, ".kladovka-meta.json") {
		return false
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." {
			return false
		}
	}
	return true
}
func (s *Server) auditedBatchDelete(ctx context.Context, actor, bucket, key string) *deleteFailure {
	if !validDeleteKey(key) {
		return &deleteFailure{key, "InvalidArgument", "unsupported object key"}
	}
	if ctx.Err() != nil {
		return &deleteFailure{key, "ServiceUnavailable", "batch deadline or request cancellation; retry unconfirmed keys"}
	}
	event := audit.Event{Node: s.Cfg.NodeID, ID: randomToken(), Kind: "attempt", Actor: actor, Method: "DELETE", Path: "/" + bucket + "/" + key}
	if s.Audit != nil {
		if err := s.Audit.Append(event); err != nil {
			return &deleteFailure{key, "ServiceUnavailable", "audit unavailable; object not deleted"}
		}
	}
	failure := s.deleteBatchKey(ctx, bucket, key)
	if s.Audit != nil {
		event.Kind = "result"
		event.Status = 204
		if failure != nil {
			switch failure.Code {
			case "NoSuchBucket", "NoSuchKey":
				event.Status = 404
			case "AccessDenied":
				event.Status = 403
			case "InvalidArgument":
				event.Status = 400
			case "InternalError", "InvalidObjectState":
				event.Status = 500
			default:
				event.Status = 503
			}
		}
		if err := s.Audit.Append(event); err != nil {
			log.Printf("batch delete audit result failed: %v", err)
			return &deleteFailure{key, "ServiceUnavailable", "audit result unavailable; deletion may have completed"}
		}
	}
	return failure
}
func (s *Server) deleteBatchKey(ctx context.Context, bucket, key string) *deleteFailure {
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(bucket, key) {
		base := s.Rep.PrimaryURL(bucket, key)
		if base == "" || base == s.Cfg.NodeURL {
			return &deleteFailure{key, "ServiceUnavailable", "primary unavailable"}
		}
		req, err := http.NewRequestWithContext(ctx, "DELETE", strings.TrimRight(base, "/")+"/internal/delete-primary/"+url.PathEscape(bucket)+"/"+escapeKeyPath(key), nil)
		if err != nil {
			return &deleteFailure{key, "ServiceUnavailable", "primary unavailable"}
		}
		s.Reg.Authorize(req)
		client := *s.Reg.Client()
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		response, err := client.Do(req)
		if err != nil {
			return &deleteFailure{key, "ServiceUnavailable", "primary deletion unconfirmed"}
		}
		defer response.Body.Close()
		if response.StatusCode == 204 {
			return nil
		}
		var remote ErrorResponse
		if xml.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&remote) == nil && remote.Code != "" {
			return &deleteFailure{key, remote.Code, remote.Message}
		}
		return &deleteFailure{key, "ServiceUnavailable", "primary deletion unconfirmed; upgrade all legacy nodes"}
	}
	err := s.Store.DeleteObject(bucket, key)
	if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
		_, code, message := mapStorageErr(err)
		return &deleteFailure{key, code, message}
	}
	if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
		if err = s.Rep.ReplicateDelete(ctx, bucket, key); err != nil {
			metrics.RecordReplicationFailed()
			return &deleteFailure{key, "ServiceUnavailable", "local delete completed but replica deletion unconfirmed"}
		}
		metrics.RecordReplicationOK()
	}
	metrics.RecordS3Operation("DeleteObject", bucket)
	return nil
}
func (s *Server) handlePrimaryDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "DELETE required", 405)
		return
	}
	bucket, key, ok := splitPolkaKey(strings.TrimPrefix(r.URL.Path, "/internal/delete-primary/"))
	if s.Consensus != nil || !ok || !validDeleteKey(key) || (s.Cfg.ReplicationFactor > 1 && !s.Rep.IsPrimary(bucket, key)) {
		http.Error(w, "not an eligible legacy primary", 409)
		return
	}
	// Existing internal authentication and audit middleware protect this route.
	s.deleteObject(w, r, bucket, key)
}
