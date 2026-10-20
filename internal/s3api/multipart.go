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
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/rkislov/kladovka/internal/metrics"
	"github.com/rkislov/kladovka/internal/storage"
)

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

func (s *Server) handleObjectMultipart(w http.ResponseWriter, r *http.Request, polka, key string) bool {
	q := r.URL.Query()
	_, hasUploads := q["uploads"]
	uploadID := q.Get("uploadId")
	partNumber := q.Get("partNumber")
	if uploadID != "" {
		release, err := s.Store.PinMultipart(uploadID)
		if err != nil {
			writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), r.URL.Path)
			return true
		}
		defer release()
		if err := s.Store.ValidateMultipartTarget(uploadID, polka, key); err != nil {
			writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), r.URL.Path)
			return true
		}
	}

	switch r.Method {
	case http.MethodPost:
		if hasUploads {
			ct := r.Header.Get("Content-Type")
			id, err := s.Store.CreateMultipartUpload(polka, key, ct)
			if err != nil {
				st, code, msg := mapStorageErr(err)
				writeS3Error(w, st, code, msg, r.URL.Path)
				return true
			}
			writeXML(w, http.StatusOK, initiateMultipartUploadResult{
				Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
				Bucket:   polka,
				Key:      key,
				UploadID: id,
			})
			return true
		}
		if uploadID != "" {
			body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
			if err != nil {
				writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), r.URL.Path)
				return true
			}
			var req completeMultipartUploadRequest
			if err := xml.Unmarshal(body, &req); err != nil {
				writeS3Error(w, http.StatusBadRequest, "MalformedXML", err.Error(), r.URL.Path)
				return true
			}
			if err := verifyPayload(r, body); err != nil {
				writeS3Error(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", err.Error(), r.URL.Path)
				return true
			}
			parts := make([]storage.MultipartPart, 0, len(req.Parts))
			for _, p := range req.Parts {
				parts = append(parts, storage.MultipartPart{PartNumber: p.PartNumber, ETag: p.ETag})
			}
			available, _ := s.Store.ListParts(uploadID)
			sizes := map[int]int64{}
			for _, p := range available {
				sizes[p.PartNumber] = p.Size
			}
			var total int64
			for _, p := range parts {
				total += sizes[p.PartNumber]
			}
			if s.Cfg.MaxObjectSize > 0 && total > s.Cfg.MaxObjectSize {
				writeS3Error(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "object too large", r.URL.Path)
				return true
			}
			meta, err := s.Store.AssembleMultipartUpload(uploadID, parts)
			if err != nil {
				if errors.Is(err, storage.ErrNoSuchUpload) {
					writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), r.URL.Path)
					return true
				}
				status, code, msg := mapStorageErr(err)
				if status == http.StatusInternalServerError && !errors.Is(err, storage.ErrIntegrity) {
					status, code, msg = http.StatusBadRequest, "InvalidRequest", err.Error()
				}
				writeS3Error(w, status, code, msg, r.URL.Path)
				return true
			}
			if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 {
				rc, _, e := s.Store.GetObject(polka, key)
				if e == nil {
					e = s.Rep.ReplicatePut(r.Context(), polka, key, rc.(io.ReadSeeker), meta.ContentType, meta.ETag)
					rc.Close()
				}
				if e != nil {
					metrics.RecordReplicationFailed()
					writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "multipart stored locally; replication incomplete", r.URL.Path)
					return true
				}
				metrics.RecordReplicationOK()
				metrics.RecordReplicationPublished(s.Cfg.ReplicationFactor - 1)
			}
			if err := s.Store.AbortMultipartUpload(uploadID); err != nil {
				writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
				return true
			}
			metrics.RecordS3Operation("CompleteMultipartUpload", polka)
			metrics.RecordBytesUploaded(polka, meta.Size)
			writeXML(w, http.StatusOK, completeMultipartUploadResult{
				Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
				Location: "/" + polka + "/" + key,
				Bucket:   polka,
				Key:      key,
				ETag:     meta.ETag,
			})
			return true
		}
	case http.MethodPut:
		if uploadID != "" && partNumber != "" {
			n, err := strconv.Atoi(partNumber)
			if err != nil {
				writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "bad partNumber", r.URL.Path)
				return true
			}
			body, _, err := s.spoolBody(r, true)
			if err != nil {
				s.spoolFailure(w, r, err)
				return true
			}
			defer removeSpool(body)
			part, err := s.Store.UploadPart(uploadID, n, body)
			if err != nil {
				if errors.Is(err, storage.ErrNoSuchUpload) {
					writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), r.URL.Path)
					return true
				}
				writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), r.URL.Path)
				return true
			}
			w.Header().Set("ETag", part.ETag)
			w.WriteHeader(http.StatusOK)
			return true
		}
	case http.MethodDelete:
		if uploadID != "" {
			if err := s.Store.AbortMultipartUpload(uploadID); err != nil {
				if errors.Is(err, storage.ErrNoSuchUpload) {
					writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), r.URL.Path)
					return true
				}
				writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
				return true
			}
			w.WriteHeader(http.StatusNoContent)
			return true
		}
	case http.MethodGet:
		if uploadID != "" {
			parts, err := s.Store.ListParts(uploadID)
			if err != nil {
				if errors.Is(err, storage.ErrNoSuchUpload) {
					writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), r.URL.Path)
					return true
				}
				writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error(), r.URL.Path)
				return true
			}
			type partXML struct {
				PartNumber   int    `xml:"PartNumber"`
				LastModified string `xml:"LastModified"`
				ETag         string `xml:"ETag"`
				Size         int64  `xml:"Size"`
			}
			type listPartsResult struct {
				XMLName  xml.Name  `xml:"ListPartsResult"`
				Xmlns    string    `xml:"xmlns,attr"`
				Bucket   string    `xml:"Bucket"`
				Key      string    `xml:"Key"`
				UploadID string    `xml:"UploadId"`
				Parts    []partXML `xml:"Part"`
			}
			out := listPartsResult{
				Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
				Bucket:   polka,
				Key:      key,
				UploadID: uploadID,
			}
			for _, p := range parts {
				out.Parts = append(out.Parts, partXML{
					PartNumber:   p.PartNumber,
					LastModified: p.LastModified.Format(time.RFC3339),
					ETag:         p.ETag,
					Size:         p.Size,
				})
			}
			writeXML(w, http.StatusOK, out)
			return true
		}
	}
	return false
}
