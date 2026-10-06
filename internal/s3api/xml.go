// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"github.com/rkislov/kladovka/internal/storage"
)

// ErrorResponse is S3-compatible XML error.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(v)
}

func writeS3Error(w http.ResponseWriter, status int, code, message, resource string) {
	writeXML(w, status, ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: "kladovka",
	})
}

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucketEntry `xml:"Bucket"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	KeyCount       int            `xml:"KeyCount"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []objectEntry  `xml:"Contents"`
	NextContToken  string         `xml:"NextContinuationToken,omitempty"`
	ContToken      string         `xml:"ContinuationToken,omitempty"`
}

type objectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func s3Time(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func listBucketsXML(names []string) listAllMyBucketsResult {
	now := s3Time(time.Now())
	entries := make([]bucketEntry, 0, len(names))
	for _, n := range names {
		entries = append(entries, bucketEntry{Name: n, CreationDate: now})
	}
	return listAllMyBucketsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner:   owner{ID: "kladovka", DisplayName: "kladovka"},
		Buckets: buckets{Bucket: entries},
	}
}

func listObjectsXML(polka, prefix, cont string, maxKeys int, objs []storage.ObjectMeta, truncated bool) listBucketResult {
	contents := make([]objectEntry, 0, len(objs))
	for _, o := range objs {
		contents = append(contents, objectEntry{
			Key:          o.Key,
			LastModified: s3Time(o.LastModified),
			ETag:         o.ETag,
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	res := listBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        polka,
		Prefix:      prefix,
		KeyCount:    len(contents),
		MaxKeys:     maxKeys,
		IsTruncated: truncated,
		Contents:    contents,
		ContToken:   cont,
	}
	if truncated && len(objs) > 0 {
		res.NextContToken = objs[len(objs)-1].Key
	}
	return res
}

func mapStorageErr(err error) (int, string, string) {
	switch err {
	case storage.ErrPolkaNotFound:
		return http.StatusNotFound, "NoSuchBucket", "The specified shelf (полка) does not exist"
	case storage.ErrPolkaExists:
		return http.StatusConflict, "BucketAlreadyOwnedByYou", "The shelf (полка) already exists"
	case storage.ErrObjectNotFound:
		return http.StatusNotFound, "NoSuchKey", "The specified key does not exist"
	case storage.ErrPolkaNotEmpty:
		return http.StatusConflict, "BucketNotEmpty", "The shelf (полка) you tried to delete is not empty"
	default:
		return http.StatusInternalServerError, "InternalError", fmt.Sprintf("%v", err)
	}
}
