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
type principalHolder struct {
	value auth.Principal
	set   bool
}

func withPrincipalHolder(ctx context.Context) context.Context {
	return context.WithValue(ctx, principalContextKey{}, &principalHolder{})
}
func adminPrincipal(user string) auth.Principal { return auth.Principal{ID: user, Role: "admin"} }

func requestPrincipal(r *http.Request) (auth.Principal, bool) {
	p, ok := r.Context().Value(principalContextKey{}).(*principalHolder)
	if !ok {
		return auth.Principal{}, false
	}
	return p.value, p.set
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
	if err := s.RecordPolicy(p); err != nil {
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
	holder, ok := r.Context().Value(principalContextKey{}).(*principalHolder)
	if !ok {
		*r = *r.WithContext(withPrincipalHolder(r.Context()))
		holder = r.Context().Value(principalContextKey{}).(*principalHolder)
	}
	holder.value, holder.set = p, true
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
