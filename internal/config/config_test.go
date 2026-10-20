// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package config

import "testing"

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, entry := range []struct{ key, value string }{{"HEALTH_INTERVAL_SEC", "0"}, {"HEALTH_INTERVAL_SEC", "oops"}, {"MAX_OBJECT_SIZE", "-1"}, {"REPLICATION_FACTOR", "0"}, {"REPLICATION_FACTOR", "3"}, {"TLS_MIN_VERSION", "1.1"}, {"CLUSTER_VERIFY_TLS", "maybe"}, {"NODE_URL", "ftp://host"}, {"MAX_CONCURRENT_UPLOADS", "0"}, {"MAX_CONCURRENT_UPLOADS", "2048"}, {"MAX_TEMP_BYTES", "-1"}, {"STORAGE_QUOTA_BYTES", "-1"}, {"MIN_FREE_DISK_BYTES", "-1"}, {"MULTIPART_TTL_SEC", "0"}, {"CLEANUP_INTERVAL_SEC", "0"}, {"VERIFY_OBJECT_CHECKSUM", "maybe"}, {"SESSION_TTL_SEC", "0"}, {"SESSION_TTL_SEC", "59"}, {"SESSION_TTL_SEC", "86401"}, {"SESSION_TTL_SEC", "oops"}} {
		t.Run(entry.key+entry.value, func(t *testing.T) {
			for _, key := range []string{"LISTEN_ADDR", "DATA_ROOT", "REGION", "ACCESS_KEY", "SECRET_KEY", "NODE_ID", "NODE_URL", "PEER_NODES", "REPLICATION_FACTOR", "CLUSTER_VERIFY_TLS", "HEALTH_INTERVAL_SEC", "MAX_OBJECT_SIZE", "TLS_CERT_FILE", "TLS_KEY_FILE", "TLS_MIN_VERSION", "MAX_CONCURRENT_UPLOADS", "MAX_TEMP_BYTES", "STORAGE_QUOTA_BYTES", "MIN_FREE_DISK_BYTES", "MULTIPART_TTL_SEC", "CLEANUP_INTERVAL_SEC", "VERIFY_OBJECT_CHECKSUM", "ADMIN_USER", "ADMIN_PASSWORD", "ACCESS_POLICY_FILE", "CLUSTER_TOKEN", "SESSION_TTL_SEC", "AUDIT_LOG_FILE"} {
				t.Setenv(key, "")
			}
			t.Setenv(entry.key, entry.value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestSeparateAdministratorConfiguration(t *testing.T) {
	for _, tc := range []struct {
		user, password, policy, token string
		valid                         bool
	}{
		{"operator", "long-admin-password", "", "", true},
		{"operator", "", "", "", false},
		{"", "long-admin-password", "", "", false},
		{"", "", "/policy.json", "token", false},
		{"operator", "short", "/policy.json", "token", false},
		{"operator", "long-admin-password", "/policy.json", "", false},
		{"operator", "long-admin-password", "/policy.json", "token", true},
	} {
		t.Run(tc.user+tc.password+tc.policy+tc.token, func(t *testing.T) {
			t.Setenv("ADMIN_USER", tc.user)
			t.Setenv("ADMIN_PASSWORD", tc.password)
			t.Setenv("ACCESS_POLICY_FILE", tc.policy)
			t.Setenv("CLUSTER_TOKEN", tc.token)
			t.Setenv("PEER_NODES", "")
			t.Setenv("REPLICATION_FACTOR", "1")
			_, err := Load()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

func TestConsensusConfiguration(t *testing.T) {
	setup := func(t *testing.T) {
		t.Setenv("LISTEN_ADDR", "127.0.0.1:18081")
		t.Setenv("RAFT_LISTEN_ADDR", "")
		t.Setenv("CLUSTER_MODE", "consensus")
		t.Setenv("RAFT_BOOTSTRAP", "true")
		t.Setenv("RAFT_JOIN", "")
		t.Setenv("RAFT_REPLACEMENT_ADDR", "")
		t.Setenv("RAFT_CLUSTER_ID", "test-new-cluster")
		t.Setenv("NODE_ID", "127.0.0.1:18081")
		t.Setenv("NODE_URL", "http://127.0.0.1:18081")
		t.Setenv("PEER_NODES", "http://127.0.0.1:18082,http://127.0.0.1:18083")
		t.Setenv("REPLICATION_FACTOR", "3")
		t.Setenv("RAFT_PEERS", `{"127.0.0.1:18081":"127.0.0.1:19081","127.0.0.1:18082":"127.0.0.1:19082","127.0.0.1:18083":"127.0.0.1:19083"}`)
		t.Setenv("CLUSTER_TOKEN", "test-cluster-token-strong")
		t.Setenv("MAX_OBJECT_SIZE", "1073741824")
		t.Setenv("RAFT_TLS_CERT_FILE", "")
		t.Setenv("RAFT_TLS_KEY_FILE", "")
		t.Setenv("RAFT_TLS_CA_FILE", "")
		t.Setenv("CLUSTER_VERIFY_TLS", "true")
		t.Setenv("ACCESS_POLICY_FILE", "")
		t.Setenv("ADMIN_USER", "")
		t.Setenv("ADMIN_PASSWORD", "")
	}
	t.Run("valid", func(t *testing.T) {
		setup(t)
		c, e := Load()
		if e != nil || !c.RaftBootstrap || c.RaftListen != "127.0.0.1:19081" {
			t.Fatalf("config: %+v %v", c, e)
		}
	})
	t.Run("additional slot retains original seeds", func(t *testing.T) {
		setup(t)
		t.Setenv("RAFT_BOOTSTRAP", "false")
		t.Setenv("RAFT_JOIN", "true")
		t.Setenv("NODE_ID", "127.0.0.1:18084")
		t.Setenv("NODE_URL", "http://127.0.0.1:18084")
		t.Setenv("LISTEN_ADDR", "127.0.0.1:18084")
		t.Setenv("PEER_NODES", "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083")
		t.Setenv("RAFT_REPLACEMENT_ADDR", "127.0.0.1:19084")
		c, e := Load()
		if e != nil || c.ReplicationFactor != 3 || len(c.RaftPeers) != 3 {
			t.Fatalf("additional slot: %+v %v", c, e)
		}
		t.Setenv("RAFT_BOOTSTRAP", "true")
		if _, e = Load(); e == nil {
			t.Fatal("additional slot bootstrapped")
		}
	})
	t.Run("replacement", func(t *testing.T) {
		setup(t)
		t.Setenv("RAFT_BOOTSTRAP", "false")
		t.Setenv("RAFT_JOIN", "true")
		t.Setenv("RAFT_REPLACEMENT_ADDR", "127.0.0.1:19181")
		c, e := Load()
		if e != nil || c.RaftListen != "127.0.0.1:19181" || !c.RaftJoin {
			t.Fatalf("replacement config: %+v %v", c, e)
		}
		for _, address := range []string{"127.0.0.1:19081", "127.0.0.1:19082", "0.0.0.0:19181", "bad", "127.0.0.1:0", "storage.example.org:19181"} {
			t.Setenv("RAFT_REPLACEMENT_ADDR", address)
			if _, e := Load(); e == nil {
				t.Fatalf("unsafe replacement address accepted: %s", address)
			}
		}
	})
	for _, tc := range []struct{ key, value string }{
		{"RAFT_JOIN", "true"}, {"RAFT_JOIN", "invalid"}, {"REPLICATION_FACTOR", "2"}, {"RAFT_CLUSTER_ID", ""}, {"CLUSTER_TOKEN", "short"}, {"RAFT_PEERS", `{}`}, {"MAX_OBJECT_SIZE", "0"}, {"MAX_OBJECT_SIZE", "2147483648"}, {"CLUSTER_VERIFY_TLS", "false"}, {"RAFT_BOOTSTRAP", "invalid"}, {"RAFT_LISTEN_ADDR", "0.0.0.0:19081"}, {"LISTEN_ADDR", ":18081"},
		{"RAFT_PEERS", `{"127.0.0.1:18081":"0.0.0.0:19081","127.0.0.1:18082":"127.0.0.1:19082","127.0.0.1:18083":"127.0.0.1:19083"}`},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			setup(t)
			t.Setenv(tc.key, tc.value)
			if _, e := Load(); e == nil {
				t.Fatal("unsafe consensus configuration accepted")
			}
		})
	}
}
