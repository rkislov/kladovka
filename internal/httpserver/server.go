// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package httpserver starts HTTP or HTTPS for Кладовка.
package httpserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/rkislov/kladovka/internal/config"
)

// ListenAndServe starts plain HTTP or TLS depending on config.
func ListenAndServe(cfg *config.Config, handler http.Handler) error {
	return ListenAndServeContext(context.Background(), cfg, handler)
}

func ListenAndServeContext(ctx context.Context, cfg *config.Config, handler http.Handler) error {
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0,
		WriteTimeout:      0,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	done := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdown); err != nil {
				srv.Close()
			}
		case <-done:
		}
	}()
	defer close(done)
	finish := func(err error) error {
		if errors.Is(err, http.ErrServerClosed) {
			<-drained
			return nil
		}
		return err
	}
	if !cfg.TLSEnabled() {
		log.Printf("kladovka: listening HTTP on %s", cfg.ListenAddr)
		return finish(srv.ListenAndServe())
	}

	minVer := tls.VersionTLS12
	if cfg.TLSMinVersion == "1.3" {
		minVer = tls.VersionTLS13
	}
	srv.TLSConfig = &tls.Config{
		MinVersion:               uint16(minVer),
		PreferServerCipherSuites: true,
		CurvePreferences: []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
		},
	}
	log.Printf("kladovka: listening HTTPS on %s (cert=%s)", cfg.ListenAddr, cfg.TLSCertFile)
	return finish(srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile))
}

// ValidateTLSFiles checks that cert/key pair is loadable when TLS is enabled.
func ValidateTLSFiles(cfg *config.Config) error {
	if !cfg.TLSEnabled() {
		return nil
	}
	_, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return fmt.Errorf("load TLS certificate: %w", err)
	}
	return nil
}
