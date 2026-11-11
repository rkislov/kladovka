// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rkislov/kladovka/internal/storage"
)

type uploadKey struct{}
type uploadReservation struct{ held int64 }

func (s *Server) limitUploads(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut && !(r.Method == http.MethodPost && (r.URL.Query().Has("uploads") || r.URL.Query().Has("uploadId") || r.URL.Query().Has("delete"))) {
			next.ServeHTTP(w, r)
			return
		}
		// A streaming proxy does not spool data locally. Reserving a disk slot here
		// could block the primary from sending a replica back to this same node.
		if s.Consensus == nil && s.Cfg.ReplicationFactor > 1 && s.Rep != nil && !strings.HasPrefix(r.URL.Path, "/internal/") {
			path := strings.TrimPrefix(r.URL.Path, "/")
			if strings.HasPrefix(r.URL.Path, "/api/object/") {
				path = strings.TrimPrefix(r.URL.Path, "/api/object/")
			}
			polka, key, ok := splitPolkaKey(path)
			if ok && !s.Rep.IsPrimary(polka, key) {
				next.ServeHTTP(w, r)
				return
			}
		}
		s.uploadOnce.Do(func() {
			limit := s.Cfg.MaxConcurrentUploads
			if limit <= 0 {
				limit = 4
			}
			s.uploadSlots = make(chan struct{}, limit)
		})
		select {
		case s.uploadSlots <- struct{}{}:
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many concurrent uploads", http.StatusServiceUnavailable)
			return
		}
		reserved := &uploadReservation{}
		defer func() { s.tempMu.Lock(); s.tempBytes -= reserved.held; s.tempMu.Unlock(); <-s.uploadSlots }()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), uploadKey{}, reserved)))
	})
}

type tempBudgetWriter struct {
	server  *Server
	request *http.Request
	dst     io.Writer
}

func (w *tempBudgetWriter) Write(body []byte) (int, error) {
	s := w.server
	reservation, _ := w.request.Context().Value(uploadKey{}).(*uploadReservation)
	if reservation == nil {
		return 0, fmt.Errorf("upload reservation missing")
	}
	s.tempMu.Lock()
	if s.Cfg.MaxTempBytes > 0 && int64(len(body)) > s.Cfg.MaxTempBytes-s.tempBytes {
		s.tempMu.Unlock()
		return 0, fmt.Errorf("%w: temporary upload budget exceeded", storage.ErrQuotaExceeded)
	}
	s.tempBytes += int64(len(body))
	reservation.held += int64(len(body))
	s.tempMu.Unlock()
	if err := s.Store.CheckFreeSpace(uint64(len(body))); err != nil {
		return 0, err
	}
	n, err := w.dst.Write(body)
	if n < len(body) {
		s.tempMu.Lock()
		s.tempBytes -= int64(len(body) - n)
		reservation.held -= int64(len(body) - n)
		s.tempMu.Unlock()
	}
	return n, err
}
func (s *Server) spoolFailure(w http.ResponseWriter, r *http.Request, err error) {
	status, code := http.StatusBadRequest, "InvalidRequest"
	if err == errTooLarge {
		status, code = http.StatusRequestEntityTooLarge, "EntityTooLarge"
	} else if errors.Is(err, storage.ErrQuotaExceeded) || errors.Is(err, storage.ErrInsufficientSpace) {
		status, code = http.StatusInsufficientStorage, "InsufficientStorage"
	}
	writeS3Error(w, status, code, err.Error(), r.URL.Path)
}
