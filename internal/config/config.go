// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings for the Кладовка server.
type Config struct {
	ClusterMode   string
	RaftClusterID string
	RaftBootstrap bool
	RaftPeers     map[string]string
	RaftListen    string
	RaftTLSCert   string
	RaftTLSKey    string
	RaftTLSCA     string
	ListenAddr    string
	DataRoot      string
	Region        string

	// TLS (optional). When both CertFile and KeyFile are set, HTTPS is enabled.
	TLSCertFile   string
	TLSKeyFile    string
	TLSMinVersion string // "1.2" or "1.3"

	// S3 credentials (fallback / bootstrap)
	ClusterToken     string
	AccessKey        string
	SecretKey        string
	AccessPolicyFile string
	AdminUser        string
	AdminPassword    string
	SessionTTL       time.Duration
	AuditLogFile     string

	// Cluster
	NodeID            string
	NodeURL           string
	PeerNodes         []string // other node base URLs
	ReplicationFactor int
	ClusterVerifyTLS  bool
	HealthInterval    time.Duration

	// Limits
	MaxObjectSize        int64 // 0 = unlimited
	MaxConcurrentUploads int
	MaxTempBytes         int64
	StorageQuotaBytes    int64
	MinFreeDiskBytes     int64
	VerifyObjectChecksum bool
	MultipartTTL         time.Duration
	CleanupInterval      time.Duration
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	for _, key := range []string{"REPLICATION_FACTOR", "HEALTH_INTERVAL_SEC", "MAX_OBJECT_SIZE", "MAX_TEMP_BYTES", "STORAGE_QUOTA_BYTES", "MIN_FREE_DISK_BYTES", "MAX_CONCURRENT_UPLOADS", "MULTIPART_TTL_SEC", "CLEANUP_INTERVAL_SEC", "SESSION_TTL_SEC"} {
		if v := os.Getenv(key); v != "" {
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				return nil, fmt.Errorf("%s must be an integer", key)
			}
		}
	}
	for _, key := range []string{"REPLICATION_FACTOR", "HEALTH_INTERVAL_SEC", "MAX_CONCURRENT_UPLOADS", "MULTIPART_TTL_SEC", "CLEANUP_INTERVAL_SEC", "SESSION_TTL_SEC"} {
		if raw := os.Getenv(key); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil {
				return nil, fmt.Errorf("%s is out of range", key)
			}
			if (key == "HEALTH_INTERVAL_SEC" || key == "MULTIPART_TTL_SEC" || key == "CLEANUP_INTERVAL_SEC" || key == "SESSION_TTL_SEC") && (n <= 0 || int64(n) > int64((1<<63-1)/int64(time.Second))) {
				return nil, fmt.Errorf("HEALTH_INTERVAL_SEC is out of range")
			}
		}
	}
	for _, key := range []string{"CLUSTER_VERIFY_TLS", "VERIFY_OBJECT_CHECKSUM", "RAFT_BOOTSTRAP"} {
		if v := strings.ToLower(os.Getenv(key)); v != "" {
			switch v {
			case "1", "true", "yes", "on", "0", "false", "no", "off":
			default:
				return nil, fmt.Errorf("%s must be boolean", key)
			}
		}
	}

	cfg := &Config{
		ClusterMode:          getenv("CLUSTER_MODE", "legacy"),
		RaftBootstrap:        getenvBool("RAFT_BOOTSTRAP", false),
		RaftClusterID:        os.Getenv("RAFT_CLUSTER_ID"),
		RaftListen:           os.Getenv("RAFT_LISTEN_ADDR"),
		RaftTLSCert:          os.Getenv("RAFT_TLS_CERT_FILE"),
		RaftTLSKey:           os.Getenv("RAFT_TLS_KEY_FILE"),
		RaftTLSCA:            os.Getenv("RAFT_TLS_CA_FILE"),
		ListenAddr:           getenv("LISTEN_ADDR", ":8000"),
		DataRoot:             getenv("DATA_ROOT", "./data"),
		Region:               getenv("REGION", "us-east-1"),
		TLSCertFile:          os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:           os.Getenv("TLS_KEY_FILE"),
		TLSMinVersion:        getenv("TLS_MIN_VERSION", "1.2"),
		ClusterToken:         os.Getenv("CLUSTER_TOKEN"),
		AccessKey:            getenv("ACCESS_KEY", "kladovka"),
		SecretKey:            getenv("SECRET_KEY", "kladovka-secret"),
		AccessPolicyFile:     os.Getenv("ACCESS_POLICY_FILE"),
		AdminUser:            os.Getenv("ADMIN_USER"),
		AdminPassword:        os.Getenv("ADMIN_PASSWORD"),
		SessionTTL:           time.Duration(getenvInt("SESSION_TTL_SEC", 28800)) * time.Second,
		AuditLogFile:         os.Getenv("AUDIT_LOG_FILE"),
		NodeID:               getenv("NODE_ID", "node-1"),
		NodeURL:              getenv("NODE_URL", "http://localhost:8000"),
		ReplicationFactor:    getenvInt("REPLICATION_FACTOR", 1),
		ClusterVerifyTLS:     getenvBool("CLUSTER_VERIFY_TLS", true),
		HealthInterval:       time.Duration(getenvInt("HEALTH_INTERVAL_SEC", 10)) * time.Second,
		MaxObjectSize:        getenvInt64("MAX_OBJECT_SIZE", 0),
		MaxConcurrentUploads: getenvInt("MAX_CONCURRENT_UPLOADS", 4),
		MaxTempBytes:         getenvInt64("MAX_TEMP_BYTES", 2<<30),
		StorageQuotaBytes:    getenvInt64("STORAGE_QUOTA_BYTES", 0),
		MinFreeDiskBytes:     getenvInt64("MIN_FREE_DISK_BYTES", 64<<20),
		VerifyObjectChecksum: getenvBool("VERIFY_OBJECT_CHECKSUM", true),
		MultipartTTL:         time.Duration(getenvInt("MULTIPART_TTL_SEC", 86400)) * time.Second,
		CleanupInterval:      time.Duration(getenvInt("CLEANUP_INTERVAL_SEC", 600)) * time.Second,
	}
	if peers := strings.TrimSpace(os.Getenv("PEER_NODES")); peers != "" {
		for _, p := range strings.Split(peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.PeerNodes = append(cfg.PeerNodes, strings.TrimRight(p, "/"))
			}
		}
	}
	if cfg.SessionTTL < time.Minute || cfg.SessionTTL > 24*time.Hour {
		return nil, fmt.Errorf("SESSION_TTL_SEC must be between 60 and 86400")
	}
	if cfg.ReplicationFactor < 1 {
		return nil, fmt.Errorf("REPLICATION_FACTOR must be positive")
	}
	if (cfg.AdminUser == "") != (cfg.AdminPassword == "") {
		return nil, fmt.Errorf("ADMIN_USER and ADMIN_PASSWORD must be set together")
	}
	if cfg.AccessPolicyFile != "" && (cfg.AdminUser == "" || len(cfg.AdminPassword) < 16 || cfg.ClusterToken == "") {
		return nil, fmt.Errorf("ACCESS_POLICY_FILE requires ADMIN_USER, ADMIN_PASSWORD (at least 16 bytes) and CLUSTER_TOKEN")
	}
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("TLS_KEY_FILE required when TLS_CERT_FILE is set")
	}
	if cfg.TLSKeyFile != "" && cfg.TLSCertFile == "" {
		return nil, fmt.Errorf("TLS_CERT_FILE required when TLS_KEY_FILE is set")
	}
	if cfg.HealthInterval <= 0 {
		return nil, fmt.Errorf("HEALTH_INTERVAL_SEC must be positive")
	}
	if cfg.MaxObjectSize < 0 || cfg.MaxObjectSize == 1<<63-1 {
		return nil, fmt.Errorf("MAX_OBJECT_SIZE must not be negative")
	}
	if cfg.MaxConcurrentUploads < 1 || cfg.MaxConcurrentUploads > 1024 || cfg.MaxTempBytes < 0 || cfg.StorageQuotaBytes < 0 || cfg.MinFreeDiskBytes < 0 || cfg.MultipartTTL <= 0 || cfg.CleanupInterval <= 0 {
		return nil, fmt.Errorf("invalid upload/storage limits")
	}
	if cfg.TLSMinVersion != "1.2" && cfg.TLSMinVersion != "1.3" {
		return nil, fmt.Errorf("TLS_MIN_VERSION must be 1.2 or 1.3")
	}
	self, err := url.Parse(cfg.NodeURL)
	if err != nil || self.Host == "" || (self.Scheme != "http" && self.Scheme != "https") || self.User != nil || self.RawQuery != "" || (self.Path != "" && self.Path != "/") {
		return nil, fmt.Errorf("NODE_URL must be an http(s) base URL")
	}
	if len(cfg.PeerNodes) > 0 && cfg.NodeID != self.Host {
		return nil, fmt.Errorf("in a cluster NODE_ID must match NODE_URL host:port")
	}
	seen := map[string]bool{self.Host: true}
	for _, raw := range cfg.PeerNodes {
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || (u.Path != "" && u.Path != "/") || seen[u.Host] {
			return nil, fmt.Errorf("PEER_NODES must contain distinct http(s) base URLs excluding this node")
		}
		seen[u.Host] = true
	}
	if cfg.ReplicationFactor > len(seen) {
		return nil, fmt.Errorf("REPLICATION_FACTOR exceeds configured node count")
	}
	if cfg.ClusterMode != "legacy" && cfg.ClusterMode != "consensus" {
		return nil, fmt.Errorf("CLUSTER_MODE must be legacy or consensus")
	}
	if cfg.ClusterMode == "consensus" {
		if strconv.IntSize < 64 {
			return nil, fmt.Errorf("consensus mode requires a 64-bit build")
		}
		if err := json.Unmarshal([]byte(os.Getenv("RAFT_PEERS")), &cfg.RaftPeers); err != nil {
			return nil, fmt.Errorf("RAFT_PEERS must be a JSON object of NODE_ID to raft host:port")
		}
		if len(seen) != len(cfg.RaftPeers) || (len(seen) != 3 && len(seen) != 5) || cfg.ReplicationFactor != len(seen) {
			return nil, fmt.Errorf("consensus requires 3 or 5 voters and RF equal to voter count")
		}
		if cfg.RaftClusterID == "" || len(cfg.ClusterToken) < 16 || cfg.MaxObjectSize <= 0 || cfg.MaxObjectSize > 1<<30 || !cfg.ClusterVerifyTLS {
			return nil, fmt.Errorf("consensus requires RAFT_CLUSTER_ID, CLUSTER_TOKEN >=16 bytes, MAX_OBJECT_SIZE 1..1GiB and verified TLS")
		}
		addresses := map[string]bool{}
		loopback := true
		for id, addr := range cfg.RaftPeers {
			host, port, err := net.SplitHostPort(addr)
			n, e := strconv.Atoi(port)
			if !seen[id] || err != nil || e != nil || n < 1 || n > 65535 || addresses[addr] {
				return nil, fmt.Errorf("invalid or duplicate raft member/address")
			}
			addresses[addr] = true
			ip := net.ParseIP(host)
			if ip == nil || !ip.IsLoopback() {
				loopback = false
			}
			nodeHost, _, err := net.SplitHostPort(id)
			if err != nil {
				return nil, fmt.Errorf("consensus NODE_ID must include explicit port")
			}
			ip = net.ParseIP(nodeHost)
			if ip == nil || !ip.IsLoopback() {
				loopback = false
			}
		}
		if cfg.RaftListen == "" {
			cfg.RaftListen = cfg.RaftPeers[cfg.NodeID]
		}
		if _, _, err := net.SplitHostPort(cfg.RaftListen); err != nil {
			return nil, fmt.Errorf("invalid RAFT_LISTEN_ADDR")
		}
		tlsFiles := cfg.RaftTLSCert != "" && cfg.RaftTLSKey != "" && cfg.RaftTLSCA != ""
		listenHost, _, _ := net.SplitHostPort(cfg.RaftListen)
		listenIP := net.ParseIP(listenHost)
		if !tlsFiles && (listenIP == nil || !listenIP.IsLoopback()) {
			return nil, fmt.Errorf("plaintext raft must listen only on loopback")
		}
		httpHost, _, listenErr := net.SplitHostPort(cfg.ListenAddr)
		httpIP := net.ParseIP(httpHost)
		if listenErr != nil {
			return nil, fmt.Errorf("invalid consensus LISTEN_ADDR")
		}
		if (httpIP == nil || !httpIP.IsLoopback()) && self.Scheme != "https" {
			return nil, fmt.Errorf("consensus HTTP requires HTTPS or an explicit loopback listener")
		}
		if !tlsFiles && (!loopback || cfg.RaftTLSCert != "" || cfg.RaftTLSKey != "" || cfg.RaftTLSCA != "") {
			return nil, fmt.Errorf("non-loopback consensus requires RAFT_TLS_CERT_FILE, RAFT_TLS_KEY_FILE and RAFT_TLS_CA_FILE")
		}
		if !loopback {
			for _, raw := range append([]string{cfg.NodeURL}, cfg.PeerNodes...) {
				u, _ := url.Parse(raw)
				if u.Scheme != "https" {
					return nil, fmt.Errorf("non-loopback consensus requires HTTPS node URLs")
				}
			}
		}
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

func getenvInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}
