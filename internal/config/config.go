// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings for the Кладовка server.
type Config struct {
	ListenAddr string
	DataRoot   string
	Region     string

	// TLS (optional). When both CertFile and KeyFile are set, HTTPS is enabled.
	TLSCertFile string
	TLSKeyFile  string
	TLSMinVersion string // "1.2" or "1.3"

	// S3 credentials (fallback / bootstrap)
	AccessKey string
	SecretKey string

	// Cluster
	NodeID            string
	NodeURL           string
	PeerNodes         []string // other node base URLs
	ReplicationFactor int
	ClusterVerifyTLS  bool
	HealthInterval    time.Duration

	// Limits
	MaxObjectSize int64 // 0 = unlimited
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:        getenv("LISTEN_ADDR", ":8000"),
		DataRoot:          getenv("DATA_ROOT", "./data"),
		Region:            getenv("REGION", "us-east-1"),
		TLSCertFile:       os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:        os.Getenv("TLS_KEY_FILE"),
		TLSMinVersion:     getenv("TLS_MIN_VERSION", "1.2"),
		AccessKey:         getenv("ACCESS_KEY", "kladovka"),
		SecretKey:         getenv("SECRET_KEY", "kladovka-secret"),
		NodeID:            getenv("NODE_ID", "node-1"),
		NodeURL:           getenv("NODE_URL", "http://localhost:8000"),
		ReplicationFactor: getenvInt("REPLICATION_FACTOR", 1),
		ClusterVerifyTLS:  getenvBool("CLUSTER_VERIFY_TLS", true),
		HealthInterval:    time.Duration(getenvInt("HEALTH_INTERVAL_SEC", 10)) * time.Second,
		MaxObjectSize:     int64(getenvInt("MAX_OBJECT_SIZE", 0)),
	}
	if peers := strings.TrimSpace(os.Getenv("PEER_NODES")); peers != "" {
		for _, p := range strings.Split(peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.PeerNodes = append(cfg.PeerNodes, strings.TrimRight(p, "/"))
			}
		}
	}
	if cfg.ReplicationFactor < 1 {
		cfg.ReplicationFactor = 1
	}
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("TLS_KEY_FILE required when TLS_CERT_FILE is set")
	}
	if cfg.TLSKeyFile != "" && cfg.TLSCertFile == "" {
		return nil, fmt.Errorf("TLS_CERT_FILE required when TLS_KEY_FILE is set")
	}
	return cfg, nil
}

// TLSEnabled reports whether HTTPS should be used.
func (c *Config) TLSEnabled() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvBool(k string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}
