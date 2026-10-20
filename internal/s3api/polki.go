// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rkislov/kladovka/internal/storage"
)

var errClusterChange = errors.New("cluster shelf update incomplete")

func (s *Server) updatePeers(ctx context.Context, name, method string) error {
	if s.Consensus != nil || s.Cfg.ReplicationFactor <= 1 {
		return nil
	}
	for _, n := range s.Reg.Snapshot() {
		if n.NodeID == s.Reg.SelfID() {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(n.URL, "/")+"/internal/polki/"+url.PathEscape(name), nil)
		if err != nil {
			return err
		}
		s.Reg.Authorize(req)
		resp, err := s.Reg.Client().Do(req)
		if err != nil {
			return fmt.Errorf("%w: %v", errClusterChange, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 && !(method == http.MethodDelete && resp.StatusCode == 404) {
			return fmt.Errorf("%w: node %s: %s", errClusterChange, n.NodeID, resp.Status)
		}
	}
	return nil
}
func (s *Server) createPolka(ctx context.Context, name string) error {
	err := s.Store.CreatePolka(name)
	if err != nil && !errors.Is(err, storage.ErrPolkaExists) {
		return err
	}
	if e := s.updatePeers(ctx, name, http.MethodPut); e != nil {
		return e
	}
	return err
}
func (s *Server) deletePolka(ctx context.Context, name string) error {
	if err := s.Store.DeletePolka(name); err != nil && !errors.Is(err, storage.ErrPolkaNotFound) {
		return err
	}
	return s.updatePeers(ctx, name, http.MethodDelete)
}
func (s *Server) handleInternalPolka(w http.ResponseWriter, r *http.Request) {
	if s.Consensus != nil {
		http.Error(w, "legacy shelf API disabled in consensus mode", 409)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/internal/polki/")
	var err error
	switch r.Method {
	case http.MethodPut:
		err = s.Store.CreatePolka(name)
		if errors.Is(err, storage.ErrPolkaExists) {
			err = nil
		}
	case http.MethodDelete:
		err = s.Store.DeletePolka(name)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		status, _, _ := mapStorageErr(err)
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
