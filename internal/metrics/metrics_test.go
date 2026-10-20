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
	UpdateConsensus(true, true, 21, 2, 2)
	UpdateClusterNode("node-1", true, 42)

	rr := httptest.NewRecorder()
	Handler(nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		"kladovka_http_requests_total",
		"kladovka_s3_operations_total",
		"kladovka_storage_used_bytes 42",
		"kladovka_cluster_node_healthy",
		"kladovka_consensus_applied_index 21",
		"kladovka_consensus_repair_pending 2",
		"kladovka_consensus_quorum_required 2",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}

func TestRemovedNodeSeriesAndDiskObservation(t *testing.T) {
	UpdateClusterNode("retired", false, 77)
	UpdateClusterNode("retained", true, 88)
	RetainClusterNodes(map[string]bool{"retained": true})
	UpdateDiskObservation(123, true)
	UpdateConsensusObservations(map[string]uint64{"membership_pending": 1, "repair_error": 1})
	rr := httptest.NewRecorder()
	Handler(nil).ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	body := rr.Body.String()
	if strings.Contains(body, `node_id="retired"`) {
		t.Fatal("retired metric still exported")
	}
	for _, want := range []string{`node_id="retained"`, "kladovka_storage_available_bytes 123", "kladovka_storage_observation_ok 1", "kladovka_consensus_membership_pending 1"} {
		if !strings.Contains(body, want) {
			t.Fatal("missing", want)
		}
	}
	UpdateDiskObservation(0, false)
	if Collect().StorageObservationOK {
		t.Fatal("failed disk observation reported as valid")
	}
}
