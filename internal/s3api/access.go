// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"context"
	"net/http"
	"strings"

	"github.com/rkislov/kladovka/internal/auth"
)

type principalContextKey struct{}

func requestPrincipal(r *http.Request) (auth.Principal, bool) {
	p, ok := r.Context().Value(principalContextKey{}).(auth.Principal)
	return p, ok
}

func (s *Server) adminCredentials() (string, string) {
	if s.Cfg.AdminUser != "" {
		return s.Cfg.AdminUser, s.Cfg.AdminPassword
	}
	return s.Cfg.AccessKey, s.Cfg.SecretKey
}

// authenticateSigned uses the same policy snapshot for the signature and rights.
// Broken or missing policy files fail closed, never restoring bootstrap access.
func (s *Server) authenticateSigned(r *http.Request) (auth.Principal, error) {
	if s.Cfg.AccessPolicyFile == "" {
		id, err := s.Auth.AuthenticateRequest(r, r.Header.Get("X-Amz-Content-Sha256"))
		return auth.Principal{ID: string(id), Role: "admin"}, err
	}
	p, err := auth.LoadPolicy(s.Cfg.AccessPolicyFile)
	if err != nil {
		return auth.Principal{}, err
	}
	a := auth.Authenticator{Region: s.Cfg.Region, Creds: p.Credentials()}
	id, err := a.AuthenticateRequest(r, r.Header.Get("X-Amz-Content-Sha256"))
	if err != nil {
		return auth.Principal{}, err
	}
	return p.Principal(id)
}

func allowedRequest(p auth.Principal, r *http.Request) bool {
	if p.Role == "admin" {
		return true
	}
	path := r.URL.Path
	read := r.Method == http.MethodGet || r.Method == http.MethodHead
	if path == "/api/polki" || path == "/api/polki/" || path == "/" {
		return r.Method == http.MethodGet
	}
	var shelf string
	var object bool
	switch {
	case strings.HasPrefix(path, "/api/object/"):
		shelf, _, object = splitPolkaKey(strings.TrimPrefix(path, "/api/object/"))
	case strings.HasPrefix(path, "/api/polki/"):
		shelf = strings.Trim(strings.TrimPrefix(path, "/api/polki/"), "/")
	case strings.HasPrefix(path, "/api/"), strings.HasPrefix(path, "/ui"), strings.HasPrefix(path, "/internal/"), path == "/metrics", path == "/cluster", path == "/healthz":
		return false
	default:
		shelf, _, object = splitPolkaKey(path)
	}
	// Shelf creation/deletion is admin-only. Multipart metadata belongs to writers.
	if read && !r.URL.Query().Has("uploadId") && !r.URL.Query().Has("uploads") {
		return p.CanRead(shelf)
	}
	return object && p.CanWrite(shelf)
}

func setPrincipal(r *http.Request, p auth.Principal) {
	*r = *r.WithContext(context.WithValue(r.Context(), principalContextKey{}, p))
}

func visibleShelves(r *http.Request, names []string) []string {
	p, ok := requestPrincipal(r)
	if !ok || p.Role == "admin" {
		return names
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if p.CanRead(n) {
			out = append(out, n)
		}
	}
	return out
}
