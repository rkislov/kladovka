// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/rkislov/kladovka/internal/cluster"
)

func secureEqual(a, b string) bool {
	x, y := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

// protect separates browser/admin authentication from S3 and inter-node credentials.
func (s *Server) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasPrefix(path, "/internal/") {
			token := cluster.SharedToken(s.Cfg.ClusterToken, s.Cfg.SecretKey)
			if !secureEqual(r.Header.Get("Authorization"), "Bearer "+token) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		} else if path == "/ui" || strings.HasPrefix(path, "/ui/") || strings.HasPrefix(path, "/api/") || path == "/metrics" || path == "/cluster" {
			user, pass, ok := r.BasicAuth()
			if !ok || !secureEqual(user, s.Cfg.AccessKey) || !secureEqual(pass, s.Cfg.SecretKey) {
				w.Header().Set("WWW-Authenticate", `Basic realm="Kladovka", charset="UTF-8"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				origin := r.Header.Get("Origin")
				if origin != "" {
					u, e := url.Parse(origin)
					if e != nil || u.Host != r.Host {
						http.Error(w, "cross-origin request denied", http.StatusForbidden)
						return
					}
				}
				if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
					http.Error(w, "cross-site request denied", http.StatusForbidden)
					return
				}
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
	f, err := os.CreateTemp("", "kladovka-upload-*")
	if err != nil {
		return nil, 0, err
	}
	fail := func(e error) (*os.File, int64, error) { f.Close(); os.Remove(f.Name()); return nil, 0, e }
	hash := sha256.New()
	var src io.Reader = r.Body
	if s.Cfg.MaxObjectSize > 0 {
		src = io.LimitReader(src, s.Cfg.MaxObjectSize+1)
	}
	n, err := io.Copy(io.MultiWriter(f, hash), src)
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
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseCounter) Write(body []byte) (int, error) {
	n, e := w.ResponseWriter.Write(body)
	w.bytes += int64(n)
	return n, e
}
