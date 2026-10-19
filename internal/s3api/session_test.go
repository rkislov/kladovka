// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").
package s3api

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func login(t *testing.T, h http.Handler) (*http.Cookie, string) {
	t.Helper()
	r := httptest.NewRequest("POST", "/api/session", strings.NewReader(`{"user":"test","password":"secret"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("login: %d %s", w.Code, w.Body.String())
	}
	var body struct {
		CSRF string `json:"csrf"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || body.CSRF == "" {
		t.Fatal("missing session")
	}
	return cookies[0], body.CSRF
}
func sessionCall(h http.Handler, c *http.Cookie, csrf, method, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(`{"name":"demo"}`))
	if c != nil {
		r.AddCookie(c)
	}
	if csrf != "" {
		r.Header.Set("X-CSRF-Token", csrf)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestSessionLifecycleCSRFAndExpiry(t *testing.T) {
	s, h := testServer(t, 100)
	s.Cfg.SessionTTL = time.Hour
	c, csrf := login(t, h)
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.MaxAge != 3600 {
		t.Fatalf("unsafe cookie: %+v", c)
	}
	for _, path := range []string{"/ui/", "/api/stats", "/api/session", "/metrics"} {
		if w := sessionCall(h, c, "", "GET", path); w.Code != 200 {
			t.Fatalf("session GET %s: %d", path, w.Code)
		}
	}
	for _, token := range []string{"", "wrong"} {
		if w := sessionCall(h, c, token, "POST", "/api/polki"); w.Code != 403 {
			t.Fatal("missing CSRF accepted")
		}
	}
	if w := sessionCall(h, c, csrf, "POST", "/api/polki"); w.Code != 201 {
		t.Fatalf("CSRF write: %d %s", w.Code, w.Body.String())
	}
	for _, origin := range []string{"https://evil.example", "https://example.com"} {
		r := httptest.NewRequest("DELETE", "/api/polki/demo", nil)
		r.AddCookie(c)
		r.Header.Set("X-CSRF-Token", csrf)
		r.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal("cross-origin mutation accepted")
		}
	}
	if w := sessionCall(h, c, "", "DELETE", "/api/session"); w.Code != 403 {
		t.Fatal("logout without CSRF accepted")
	}
	if w := sessionCall(h, c, csrf, "DELETE", "/api/session"); w.Code != 204 || w.Result().Cookies()[0].MaxAge != -1 {
		t.Fatal("logout failed")
	}
	if w := sessionCall(h, c, "", "GET", "/api/stats"); w.Code != 401 {
		t.Fatal("logged out token accepted")
	}
	c, csrf = login(t, h)
	key := sha256.Sum256([]byte(c.Value))
	s.sessionMu.Lock()
	v := s.sessions[key]
	v.Expires = time.Now().Add(-time.Second)
	s.sessions[key] = v
	s.sessionMu.Unlock()
	if w := sessionCall(h, c, csrf, "GET", "/api/session"); w.Code != 401 {
		t.Fatal("expired token accepted")
	}
	c, _ = login(t, h)
	s.Cfg.AdminUser = "operator"
	s.Cfg.AdminPassword = "changed-password"
	if w := sessionCall(h, c, "", "GET", "/api/stats"); w.Code != 401 {
		t.Fatal("old credentials session accepted")
	}
}
func TestSessionRotationRestartAndLoginProtection(t *testing.T) {
	_, h := testServer(t, 100)
	c, _ := login(t, h)
	r := httptest.NewRequest("POST", "/api/session", strings.NewReader(`{"user":"test","password":"secret"}`))
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(c)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := sessionCall(h, c, "", "GET", "/api/session"); w.Code != 401 {
		t.Fatal("pre-login token not rotated")
	}
	current := w.Result().Cookies()[0]
	_, restarted := testServer(t, 100)
	if w := sessionCall(restarted, current, "", "GET", "/api/session"); w.Code != 401 {
		t.Fatal("session survived a new server")
	}
	for _, tc := range []struct {
		content, origin string
		status          int
	}{{"text/plain", "", 415}, {"application/json", "https://evil.example", 403}, {"application/json", "null", 403}} {
		r := httptest.NewRequest("POST", "/api/session", strings.NewReader(`{"user":"test","password":"secret"}`))
		r.Header.Set("Content-Type", tc.content)
		r.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("login protection: %d", w.Code)
		}
	}
	// Bounded per-peer login attempts, ignoring untrusted X-Forwarded-For.
	_, h = testServer(t, 100)
	for i := 0; i < 11; i++ {
		r := httptest.NewRequest("POST", "/api/session", strings.NewReader(`{"user":"test","password":"wrong"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Forwarded-For", string(rune('a'+i)))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		expected := 401
		if i == 10 {
			expected = 429
		}
		if w.Code != expected {
			t.Fatalf("attempt %d: %d", i, w.Code)
		}
	}
}
func TestSecureSessionCookie(t *testing.T) {
	s, h := testServer(t, 100)
	s.Cfg.NodeURL = "https://example.com"
	c, _ := login(t, h)
	if !c.Secure {
		t.Fatal("HTTPS cookie not secure")
	}
}
