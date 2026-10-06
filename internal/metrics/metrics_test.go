// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");

package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsHandlerContainsSeries(t *testing.T) {
	RecordHTTP("GET", "/healthz", 200, 5*time.Millisecond)
	RecordS3Operation("PutObject", "полка")
	RecordBytesUploaded("полка", 100)
	UpdateStorage(42, 0)
	UpdateClusterNode("node-1", true, 42)

	rr := httptest.NewRecorder()
	Handler(nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		"kladovka_http_requests_total",
		"kladovka_s3_operations_total",
		"kladovka_storage_used_bytes 42",
		"kladovka_cluster_node_healthy",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}
