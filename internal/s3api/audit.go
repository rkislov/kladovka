// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").
package s3api

import (
	"crypto/sha256"
	"encoding/json"
	"log"
	"net/http"

	"github.com/rkislov/kladovka/internal/audit"
	"github.com/rkislov/kladovka/internal/auth"
)

func (s *Server) RecordPolicy(p *auth.Policy) error {
	if s.Audit == nil {
		return nil
	}
	body, _ := json.Marshal(p)
	hash := sha256.Sum256(body)
	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	if s.policySeen && hash == s.policyHash {
		return nil
	}
	users := make([]audit.User, 0, len(p.Users))
	for _, u := range p.Users {
		pub := audit.User{ID: u.ID, Role: u.Role, Polki: u.Polki, Keys: []string{}}
		for _, k := range u.Keys {
			pub.Keys = append(pub.Keys, k.AccessKey)
		}
		users = append(users, pub)
	}
	if err := s.Audit.Append(audit.Event{Node: s.Cfg.NodeID, Kind: "policy_snapshot", Actor: "local_configuration", Users: users}); err != nil {
		return err
	}
	s.policySeen, s.policyHash = true, hash
	return nil
}

func (s *Server) auditRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Audit == nil || r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		// A shared holder preserves the verified actor across request-context clones.
		r = r.WithContext(withPrincipalHolder(r.Context()))
		e := audit.Event{Node: s.Cfg.NodeID, ID: randomToken(), Kind: "attempt", Actor: "anonymous", Method: r.Method, Path: r.URL.Path}
		if err := s.Audit.Append(e); err != nil {
			http.Error(w, "audit unavailable", 503)
			return
		}
		counter := &responseCounter{ResponseWriter: w}
		next.ServeHTTP(counter, r)
		if p, ok := requestPrincipal(r); ok {
			e.Actor = p.ID
		}
		e.Kind = "result"
		e.Status = counter.status
		if e.Status == 0 {
			e.Status = 200
		}
		if err := s.Audit.Append(e); err != nil {
			log.Printf("audit result append failed: %v", err)
		}
	})
}
