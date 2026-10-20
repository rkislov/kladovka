// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"io"
	"net/http"
	"strconv"
	"strings"
)

func (s *Server) consensusStatus() any {
	if s.Consensus == nil {
		return map[string]string{"mode": "legacy"}
	}
	return s.Consensus.Status()
}
func (s *Server) consensusRouting(next http.Handler) http.Handler {
	if s.Consensus == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/api/session" || strings.HasPrefix(path, "/ui") || strings.HasPrefix(path, "/internal/") || path == "/healthz" || path == "/metrics" || path == "/cluster" {
			next.ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(path, "/api/") {
			if !s.requireAuth(w, r) {
				return
			}
		}
		if !s.Consensus.IsLeader() {
			base := s.Consensus.LeaderURL()
			hops, _ := strconv.Atoi(r.Header.Get("X-Kladovka-Forward-Hops"))
			if base == "" || base == s.Cfg.NodeURL || hops >= 2 {
				writeS3Error(w, 503, "ServiceUnavailable", "consensus leader unavailable", path)
				return
			}
			req, e := http.NewRequestWithContext(r.Context(), r.Method, strings.TrimRight(base, "/")+r.URL.EscapedPath(), r.Body)
			if e != nil {
				http.Error(w, "forwarding failed", 502)
				return
			}
			req.URL.RawQuery = r.URL.RawQuery
			req.Host = r.Host
			req.ContentLength = r.ContentLength
			req.Header = r.Header.Clone()
			req.Header.Set("X-Kladovka-Forward-Hops", strconv.Itoa(hops+1))
			if active, _ := r.Context().Value(sessionContextKey{}).(bool); active {
				req.Header.Del("Cookie")
				req.Header.Del("X-CSRF-Token")
				u, p := s.adminCredentials()
				req.SetBasicAuth(u, p)
			}
			resp, e := s.Reg.Client().Do(req)
			if e != nil {
				writeS3Error(w, 503, "ServiceUnavailable", "consensus leader unreachable", path)
				return
			}
			defer resp.Body.Close()
			for k, values := range resp.Header {
				for _, v := range values {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		if e := s.Consensus.Barrier(); e != nil {
			writeS3Error(w, 503, "ServiceUnavailable", "consensus quorum unavailable", path)
			return
		}
		next.ServeHTTP(w, r)
	})
}
