// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").
package s3api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"
)

const sessionCookie = "kladovka_session"

type browserSession struct {
	User, CSRF  string
	Expires     time.Time
	Credentials [32]byte
}
type loginWindow struct {
	Start    time.Time
	Attempts int
}
type sessionContextKey struct{}

func randomToken() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (s *Server) cookieSecure(r *http.Request) bool {
	return r.TLS != nil || strings.HasPrefix(s.Cfg.NodeURL, "https://")
}
func (s *Server) credentialsVersion() [32]byte {
	u, p := s.adminCredentials()
	return sha256.Sum256([]byte(u + "\x00" + p))
}
func (s *Server) browserSession(r *http.Request) (browserSession, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil || len(c.Value) != 64 {
		return browserSession{}, false
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	key := sha256.Sum256([]byte(c.Value))
	v, ok := s.sessions[key]
	if !ok || !time.Now().Before(v.Expires) || v.Credentials != s.credentialsVersion() {
		delete(s.sessions, key)
		return browserSession{}, false
	}
	return v, true
}
func (s *Server) sessionCookie(w http.ResponseWriter, r *http.Request, token string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", HttpOnly: true, Secure: s.cookieSecure(r), SameSite: http.SameSiteStrictMode, MaxAge: maxAge})
}
func (s *Server) allowLogin(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	now := time.Now()
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	if s.loginWindows == nil {
		s.loginWindows = map[string]loginWindow{}
	}
	for k, v := range s.loginWindows {
		if now.Sub(v.Start) >= time.Minute {
			delete(s.loginWindows, k)
		}
	}
	v, ok := s.loginWindows[host]
	if !ok {
		if len(s.loginWindows) >= 4096 {
			return false
		}
		v.Start = now
	}
	v.Attempts++
	s.loginWindows[host] = v
	return v.Attempts <= 10
}
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodPost:
		if !sameOrigin(r, s.cookieSecure(r)) {
			http.Error(w, "cross-origin request denied", 403)
			return
		}
		media, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if media != "application/json" {
			http.Error(w, "JSON required", 415)
			return
		}
		if !s.allowLogin(r) {
			w.Header().Set("Retry-After", "60")
			http.Error(w, "too many login attempts", 429)
			return
		}
		var body struct {
			User     string `json:"user"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF {
			http.Error(w, "invalid login", 400)
			return
		}
		user, password := s.adminCredentials()
		if !secureEqual(body.User, user) || !secureEqual(body.Password, password) {
			http.Error(w, "invalid credentials", 401)
			return
		}
		setPrincipal(r, adminPrincipal(user))
		ttl := s.Cfg.SessionTTL
		if ttl <= 0 {
			ttl = 8 * time.Hour
		}
		token, csrf := randomToken(), randomToken()
		now := time.Now()
		value := browserSession{User: user, CSRF: csrf, Expires: now.Add(ttl), Credentials: s.credentialsVersion()}
		s.sessionMu.Lock()
		if s.sessions == nil {
			s.sessions = map[[32]byte]browserSession{}
		}
		for k, v := range s.sessions {
			if !now.Before(v.Expires) {
				delete(s.sessions, k)
			}
		}
		if len(s.sessions) >= 1024 {
			s.sessionMu.Unlock()
			http.Error(w, "too many sessions", 503)
			return
		}
		// Rotating on login prevents session fixation and revokes the old browser token.
		if old, err := r.Cookie(sessionCookie); err == nil {
			delete(s.sessions, sha256.Sum256([]byte(old.Value)))
		}
		s.sessions[sha256.Sum256([]byte(token))] = value
		s.sessionMu.Unlock()
		s.sessionCookie(w, r, token, int(ttl/time.Second))
		setPrincipal(r, adminPrincipal(user))
		json.NewEncoder(w).Encode(map[string]any{"user": user, "csrf": csrf, "expires_at": value.Expires})
	case http.MethodGet, http.MethodDelete:
		value, ok := s.browserSession(r)
		if !ok {
			s.sessionCookie(w, r, "", -1)
			http.Error(w, "session expired", 401)
			return
		}
		setPrincipal(r, adminPrincipal(value.User))
		if r.Method == http.MethodDelete {
			if !sameOrigin(r, s.cookieSecure(r)) || !secureEqual(r.Header.Get("X-CSRF-Token"), value.CSRF) {
				http.Error(w, "CSRF token required", 403)
				return
			}
			c, _ := r.Cookie(sessionCookie)
			s.sessionMu.Lock()
			delete(s.sessions, sha256.Sum256([]byte(c.Value)))
			s.sessionMu.Unlock()
			s.sessionCookie(w, r, "", -1)
			w.WriteHeader(204)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"user": value.User, "csrf": value.CSRF, "expires_at": value.Expires})
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		http.Error(w, "method not allowed", 405)
	}
}
