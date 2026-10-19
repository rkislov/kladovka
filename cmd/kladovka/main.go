// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Command kladovka is a single-binary S3-compatible object store («Кладовка»).
// Buckets are called «полки» (shelves) in product terminology.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rkislov/kladovka/internal/auth"
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/httpserver"
	"github.com/rkislov/kladovka/internal/s3api"
	"github.com/rkislov/kladovka/internal/storage"
)

var (
	version = "0.4.0-dev"
	commit  = "dev"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("kladovka %s (%s)\n", version, commit)
		fmt.Println("Copyright 2026 Роман Сергеевич Кислов")
		fmt.Println("Licensed under the Apache License, Version 2.0")
		return
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := httpserver.ValidateTLSFiles(cfg); err != nil {
		log.Fatalf("tls: %v", err)
	}

	store, err := storage.NewWithOptions(cfg.DataRoot, storage.Options{QuotaBytes: cfg.StorageQuotaBytes, MinFreeBytes: uint64(cfg.MinFreeDiskBytes), VerifyReads: cfg.VerifyObjectChecksum, MultipartTTL: cfg.MultipartTTL})
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	reg := cluster.NewRegistry(cfg.NodeID, cfg.NodeURL, cfg.PeerNodes, cfg.ClusterVerifyTLS, cfg.HealthInterval)
	reg.SetToken(cluster.SharedToken(cfg.ClusterToken, cfg.SecretKey))
	rep := &cluster.Replicator{Registry: reg, ReplicationFactor: cfg.ReplicationFactor}

	authenticator := &auth.Authenticator{
		Region: cfg.Region,
		Creds:  auth.Credentials{cfg.AccessKey: cfg.SecretKey},
	}

	srv := &s3api.Server{
		Cfg:   cfg,
		Store: store,
		Auth:  authenticator,
		Rep:   rep,
		Reg:   reg,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go reg.StartHealthLoop(ctx)
	go func() {
		cleanup := func() {
			removed, err := store.CleanupMultipart(time.Now())
			if err != nil {
				log.Printf("multipart cleanup: %v", err)
			} else if removed > 0 {
				log.Printf("multipart cleanup: removed %d expired uploads", removed)
			}
		}
		cleanup()
		ticker := time.NewTicker(cfg.CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanup()
			}
		}
	}()

	log.Printf("kladovka %s starting: node=%s data=%s rf=%d peers=%d tls=%v",
		version, cfg.NodeID, cfg.DataRoot, cfg.ReplicationFactor, len(cfg.PeerNodes), cfg.TLSEnabled())

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpserver.ListenAndServeContext(ctx, cfg, srv.Handler())
	}()

	select {
	case <-ctx.Done():
		log.Printf("kladovka: shutting down")
		if err := <-errCh; err != nil {
			log.Printf("shutdown: %v", err)
		}
	case err := <-errCh:
		if err != nil {
			log.Fatalf("server: %v", err)
		}
	}
}
