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
