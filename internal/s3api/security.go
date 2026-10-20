// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/rkislov/kladovka/internal/auth"
	"github.com/rkislov/kladovka/internal/cluster"
)

func secureEqual(a, b string) bool {
	x, y := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

// sameOrigin rejects browser cross-site writes, including changes of scheme.
func sameOrigin(r *http.Request, secure bool) bool {
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	scheme := "http"
	if secure {
		scheme = "https"
	}
	return err == nil && u.Scheme == scheme && u.Host == r.Host && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

func (s *Server) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		ui := path == "/ui" || strings.HasPrefix(path, "/ui/")
		api := strings.HasPrefix(path, "/api/")
		if ui || api {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Referrer-Policy", "no-referrer")
			w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		}
		if path == "/api/session" || path == "/ui/login.html" || path == "/ui/login.js" || path == "/ui/app.css" {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(path, "/internal/") {
			token := cluster.SharedToken(s.Cfg.ClusterToken, s.Cfg.SecretKey)
			if !secureEqual(r.Header.Get("Authorization"), "Bearer "+token) {
				http.Error(w, "unauthorized", 401)
				return
			}
			setPrincipal(r, auth.Principal{ID: "cluster", Role: "internal"})
		} else if ui || api || path == "/metrics" || path == "/cluster" {
			value, sessionOK := s.browserSession(r)
			_, cookieErr := r.Cookie(sessionCookie)
			if sessionOK {
				setPrincipal(r, adminPrincipal(value.User))
				*r = *r.WithContext(context.WithValue(r.Context(), sessionContextKey{}, true))
				if r.Method != http.MethodGet && r.Method != http.MethodHead && !secureEqual(r.Header.Get("X-CSRF-Token"), value.CSRF) {
					http.Error(w, "CSRF token required", 403)
					return
				}
			} else if ui {
				http.Redirect(w, r, "/ui/login.html", http.StatusSeeOther)
				return
			} else if cookieErr == nil {
				s.sessionCookie(w, r, "", -1)
				http.Error(w, "session expired", 401)
				return
			} else {
				user, pass, ok := r.BasicAuth()
				adminUser, adminPass := s.adminCredentials()
				if ok && secureEqual(user, adminUser) && secureEqual(pass, adminPass) {
					setPrincipal(r, auth.Principal{ID: adminUser, Role: "admin"})
				} else if api && (strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") || r.URL.Query().Has("X-Amz-Algorithm")) {
					if !s.requireAuth(w, r) {
						return
					}
				} else {
					w.Header().Set("WWW-Authenticate", `Basic realm="Kladovka", charset="UTF-8"`)
					http.Error(w, "unauthorized", 401)
					return
				}
			}
			if r.Method != http.MethodGet && r.Method != http.MethodHead && !sameOrigin(r, s.cookieSecure(r)) {
				http.Error(w, "cross-origin request denied", 403)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

var errTooLarge = errors.New("object too large")

// spoolBody bounds disk use, validates signed payloads and avoids buffering objects in RAM.
func (s *Server) spoolBody(r *http.Request, verify bool) (*os.File, int64, error) {
	if s.Cfg.MaxObjectSize > 0 && r.ContentLength > s.Cfg.MaxObjectSize {
		return nil, 0, errTooLarge
	}
	f, err := os.CreateTemp(s.Store.UploadDir(), "upload-*.tmp")
	if err != nil {
		return nil, 0, err
	}
	fail := func(e error) (*os.File, int64, error) { f.Close(); os.Remove(f.Name()); return nil, 0, e }
	hash := sha256.New()
	var src io.Reader = r.Body
	if s.Cfg.MaxObjectSize > 0 {
		src = io.LimitReader(src, s.Cfg.MaxObjectSize+1)
	}
	n, err := io.Copy(&tempBudgetWriter{server: s, request: r, dst: io.MultiWriter(f, hash)}, src)
	if err != nil {
		return fail(err)
	}
	if s.Cfg.MaxObjectSize > 0 && n > s.Cfg.MaxObjectSize {
		return fail(errTooLarge)
	}
	signed := r.Header.Get("X-Amz-Content-Sha256")
	if verify && signed != "" && signed != "UNSIGNED-PAYLOAD" {
		if !secureEqual(strings.ToLower(signed), hex.EncodeToString(hash.Sum(nil))) {
			return fail(errors.New("payload SHA256 mismatch or unsupported streaming signature"))
		}
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return f, n, nil
}
func removeSpool(f *os.File) { f.Close(); os.Remove(f.Name()) }

func verifyPayload(r *http.Request, body []byte) error {
	signed := r.Header.Get("X-Amz-Content-Sha256")
	if signed == "" || signed == "UNSIGNED-PAYLOAD" {
		return nil
	}
	h := sha256.Sum256(body)
	if !secureEqual(strings.ToLower(signed), hex.EncodeToString(h[:])) {
		return errors.New("payload SHA256 mismatch")
	}
	return nil
}

type responseCounter struct {
	http.ResponseWriter
	bytes  int64
	status int
}

func (w *responseCounter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseCounter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, e := w.ResponseWriter.Write(body)
	w.bytes += int64(n)
	return n, e
}
