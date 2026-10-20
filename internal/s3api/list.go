// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/rkislov/kladovka/internal/metrics"
	"github.com/rkislov/kladovka/internal/storage"
)

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// This cursor is stateless and bound to the listing shape, not an authorization
// credential or a snapshot. Permissions are checked again on every request.
type listingCursor struct {
	Version int    `json:"v"`
	Scope   string `json:"s"`
	After   string `json:"a"`
}

func listingScope(bucket, prefix, delimiter string) string {
	data, _ := json.Marshal([]string{bucket, prefix, delimiter})
	sum := sha256.Sum256(data)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
func encodeListingCursor(c listingCursor) string {
	data, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(data)
}
func decodeListingCursor(token, bucket, prefix, delimiter string) (string, bool) {
	if len(token) > 65536 {
		return "", false
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c listingCursor
	if dec.Decode(&c) != nil || dec.Decode(new(any)) != io.EOF || c.Version != 1 || c.Scope != listingScope(bucket, prefix, delimiter) || c.After == "" || !strings.HasPrefix(c.After, prefix) {
		return "", false
	}
	return c.After, true
}

func listPageXML(page storage.ObjectPage) (listBucketResult, string) {
	result := listBucketResult{IsTruncated: page.Truncated}
	for _, obj := range page.Objects {
		result.Contents = append(result.Contents, objectEntry{Key: obj.Key, LastModified: s3Time(obj.LastModified), ETag: obj.ETag, Size: obj.Size, StorageClass: "STANDARD"})
	}
	for _, prefix := range page.Prefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{prefix})
	}
	return result, page.Next
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	fail := func(code, message string) { writeS3Error(w, 400, code, message, r.URL.Path) }
	if err != nil {
		fail("InvalidArgument", "invalid query encoding")
		return
	}
	for _, name := range []string{"list-type", "prefix", "delimiter", "max-keys", "encoding-type", "marker", "start-after", "continuation-token", "fetch-owner"} {
		if len(q[name]) > 1 {
			fail("InvalidArgument", "duplicate listing parameter: "+name)
			return
		}
	}
	v2 := q.Get("list-type") == "2"
	if q.Has("list-type") && !v2 {
		fail("InvalidArgument", "list-type must be 2; omit for ListObjects V1")
		return
	}
	if (v2 && q.Has("marker")) || (!v2 && (q.Has("continuation-token") || q.Has("start-after") || q.Has("fetch-owner"))) {
		fail("InvalidArgument", "parameters do not match listing version")
		return
	}
	if value := q.Get("fetch-owner"); value != "" && value != "false" {
		if value != "true" {
			fail("InvalidArgument", "fetch-owner must be true or false")
			return
		}
		writeS3Error(w, 501, "NotImplemented", "object owner listing is not supported", r.URL.Path)
		return
	}
	encoding := q.Get("encoding-type")
	if q.Has("encoding-type") && encoding != "url" {
		fail("InvalidArgument", "encoding-type must be url")
		return
	}
	limit := 1000
	if q.Has("max-keys") {
		limit, err = strconv.Atoi(q.Get("max-keys"))
		if err != nil || limit < 0 {
			fail("InvalidArgument", "max-keys must be a nonnegative integer")
			return
		}
		limit = min(limit, 1000)
	}
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	after := q.Get("marker")
	if v2 {
		after = q.Get("start-after")
	}
	if q.Has("continuation-token") {
		var ok bool
		after, ok = decodeListingCursor(q.Get("continuation-token"), bucket, prefix, delimiter)
		if !ok {
			fail("InvalidArgument", "invalid continuation token for this listing")
			return
		}
	}
	page, err := s.Store.ListObjectPage(bucket, prefix, delimiter, after, limit)
	if err != nil {
		status, code, message := mapStorageErr(err)
		writeS3Error(w, status, code, message, r.URL.Path)
		return
	}
	result, last := listPageXML(page)
	result.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	result.Name, result.Prefix, result.Delimiter, result.MaxKeys = bucket, prefix, delimiter, limit
	result.EncodingType = encoding
	operation := "ListObjects"
	if v2 {
		operation = "ListObjectsV2"
		count := len(result.Contents) + len(result.CommonPrefixes)
		result.KeyCount = &count
		result.ContToken, result.StartAfter = q.Get("continuation-token"), q.Get("start-after")
		if result.IsTruncated {
			result.NextContToken = encodeListingCursor(listingCursor{1, listingScope(bucket, prefix, delimiter), last})
		}
	} else {
		marker := q.Get("marker")
		result.Marker = &marker
		if result.IsTruncated && delimiter != "" {
			result.NextMarker = last
		}
	}
	if encoding == "url" {
		encode := func(value string) string { return strings.ReplaceAll(url.QueryEscape(value), "+", "%20") }
		result.Prefix, result.Delimiter, result.StartAfter = encode(result.Prefix), encode(result.Delimiter), encode(result.StartAfter)
		if result.Marker != nil {
			*result.Marker = encode(*result.Marker)
		}
		result.NextMarker = encode(result.NextMarker)
		for i := range result.Contents {
			result.Contents[i].Key = encode(result.Contents[i].Key)
		}
		for i := range result.CommonPrefixes {
			result.CommonPrefixes[i].Prefix = encode(result.CommonPrefixes[i].Prefix)
		}
	}
	metrics.RecordS3Operation(operation, bucket)
	writeXML(w, 200, result)
}
