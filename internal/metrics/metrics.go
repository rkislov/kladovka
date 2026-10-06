// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package metrics exposes Prometheus-compatible counters and gauges for Кладовка.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	httpRequests    sync.Map // key "method|path|status" -> *atomic.Uint64
	httpDurationSum sync.Map // key "method|path" -> *atomic.Uint64 (microseconds)
	httpDurationCnt sync.Map

	s3Ops      sync.Map // operation|polka -> *atomic.Uint64
	bytesUp    sync.Map // polka -> *atomic.Uint64
	bytesDown  sync.Map
	storageUsed atomic.Uint64
	storageTotal atomic.Uint64

	clusterHealthy sync.Map // node_id -> *atomic.Uint64 (0/1)
	clusterUsed    sync.Map // node_id -> *atomic.Uint64

	replPublished atomic.Uint64
	replFailed    atomic.Uint64
	replOK        atomic.Uint64

	startTime = time.Now()
)

func counterInc(m *sync.Map, key string, delta uint64) {
	v, _ := m.LoadOrStore(key, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(delta)
}

func counterGet(m *sync.Map, key string) uint64 {
	v, ok := m.Load(key)
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

// RecordHTTP records one finished HTTP request.
func RecordHTTP(method, pathTemplate string, status int, duration time.Duration) {
	key := method + "|" + pathTemplate + "|" + strconv.Itoa(status)
	counterInc(&httpRequests, key, 1)
	dkey := method + "|" + pathTemplate
	counterInc(&httpDurationSum, dkey, uint64(duration.Microseconds()))
	counterInc(&httpDurationCnt, dkey, 1)
}

// RecordS3Operation increments S3 operation counter (полка label).
func RecordS3Operation(operation, polka string) {
	if polka == "" {
		polka = "_"
	}
	counterInc(&s3Ops, operation+"|"+polka, 1)
}

// RecordBytesUploaded adds uploaded bytes for a полка.
func RecordBytesUploaded(polka string, n int64) {
	if n <= 0 {
		return
	}
	if polka == "" {
		polka = "_"
	}
	counterInc(&bytesUp, polka, uint64(n))
}

// RecordBytesDownloaded adds downloaded bytes for a полка.
func RecordBytesDownloaded(polka string, n int64) {
	if n <= 0 {
		return
	}
	if polka == "" {
		polka = "_"
	}
	counterInc(&bytesDown, polka, uint64(n))
}

// UpdateStorage sets local disk gauges.
func UpdateStorage(used, total uint64) {
	storageUsed.Store(used)
	storageTotal.Store(total)
}

// UpdateClusterNode sets per-node health and used bytes.
func UpdateClusterNode(nodeID string, healthy bool, usedBytes uint64) {
	var h uint64
	if healthy {
		h = 1
	}
	hv, _ := clusterHealthy.LoadOrStore(nodeID, &atomic.Uint64{})
	hv.(*atomic.Uint64).Store(h)
	uv, _ := clusterUsed.LoadOrStore(nodeID, &atomic.Uint64{})
	uv.(*atomic.Uint64).Store(usedBytes)
}

// RecordReplicationPublished increments published replication jobs.
func RecordReplicationPublished(n int) {
	if n > 0 {
		replPublished.Add(uint64(n))
	}
}

// RecordReplicationOK increments successful replica copies.
func RecordReplicationOK() { replOK.Add(1) }

// RecordReplicationFailed increments failed replica copies.
func RecordReplicationFailed() { replFailed.Add(1) }

// PathTemplate reduces cardinality of paths for labels.
func PathTemplate(path string) string {
	p := strings.Trim(path, "/")
	if p == "" {
		return "/"
	}
	parts := strings.Split(p, "/")
	switch parts[0] {
	case "healthz", "metrics", "cluster", "ui":
		return "/" + parts[0]
	case "api":
		if len(parts) > 1 {
			return "/api/" + parts[1]
		}
		return "/api"
	case "internal":
		if len(parts) > 1 {
			return "/internal/" + parts[1]
		}
		return "/internal"
	default:
		if len(parts) == 1 {
			return "/{polka}"
		}
		return "/{polka}/{key}"
	}
}

// Middleware wraps a handler and records HTTP metrics (skips /metrics itself).
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		RecordHTTP(r.Method, PathTemplate(r.URL.Path), rw.status, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Handler serves Prometheus exposition format at /metrics.
func Handler(refresh func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refresh != nil {
			refresh()
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder
		writeHelpType(&b, "kladovka_build_info", "gauge", "Build/runtime info (always 1)")
		fmt.Fprintf(&b, "kladovka_build_info{service=\"kladovka\"} 1\n")
		writeHelpType(&b, "kladovka_uptime_seconds", "gauge", "Process uptime in seconds")
		fmt.Fprintf(&b, "kladovka_uptime_seconds %f\n", time.Since(startTime).Seconds())

		writeHelpType(&b, "kladovka_http_requests_total", "counter", "Total HTTP requests")
		forEachCounter(&httpRequests, func(key string, v uint64) {
			parts := strings.Split(key, "|")
			if len(parts) != 3 {
				return
			}
			fmt.Fprintf(&b, "kladovka_http_requests_total{method=%q,path=%q,status=%q} %d\n",
				parts[0], parts[1], parts[2], v)
		})

		writeHelpType(&b, "kladovka_http_request_duration_seconds_sum", "counter", "Sum of HTTP request durations")
		writeHelpType(&b, "kladovka_http_request_duration_seconds_count", "counter", "Count of timed HTTP requests")
		forEachCounter(&httpDurationSum, func(key string, sum uint64) {
			parts := strings.Split(key, "|")
			if len(parts) != 2 {
				return
			}
			cnt := counterGet(&httpDurationCnt, key)
			fmt.Fprintf(&b, "kladovka_http_request_duration_seconds_sum{method=%q,path=%q} %f\n",
				parts[0], parts[1], float64(sum)/1e6)
			fmt.Fprintf(&b, "kladovka_http_request_duration_seconds_count{method=%q,path=%q} %d\n",
				parts[0], parts[1], cnt)
		})

		writeHelpType(&b, "kladovka_s3_operations_total", "counter", "S3 API operations by type and полка")
		forEachCounter(&s3Ops, func(key string, v uint64) {
			parts := strings.SplitN(key, "|", 2)
			if len(parts) != 2 {
				return
			}
			fmt.Fprintf(&b, "kladovka_s3_operations_total{operation=%q,polka=%q} %d\n", parts[0], parts[1], v)
		})

		writeHelpType(&b, "kladovka_s3_bytes_uploaded_total", "counter", "Bytes uploaded (PutObject)")
		forEachCounter(&bytesUp, func(polka string, v uint64) {
			fmt.Fprintf(&b, "kladovka_s3_bytes_uploaded_total{polka=%q} %d\n", polka, v)
		})
		writeHelpType(&b, "kladovka_s3_bytes_downloaded_total", "counter", "Bytes downloaded (GetObject)")
		forEachCounter(&bytesDown, func(polka string, v uint64) {
			fmt.Fprintf(&b, "kladovka_s3_bytes_downloaded_total{polka=%q} %d\n", polka, v)
		})

		writeHelpType(&b, "kladovka_storage_used_bytes", "gauge", "Local storage used bytes")
		fmt.Fprintf(&b, "kladovka_storage_used_bytes %d\n", storageUsed.Load())
		writeHelpType(&b, "kladovka_storage_total_bytes", "gauge", "Local storage total bytes (0 if unknown)")
		fmt.Fprintf(&b, "kladovka_storage_total_bytes %d\n", storageTotal.Load())

		writeHelpType(&b, "kladovka_cluster_node_healthy", "gauge", "1 if cluster node is healthy")
		forEachCounter(&clusterHealthy, func(nodeID string, v uint64) {
			fmt.Fprintf(&b, "kladovka_cluster_node_healthy{node_id=%q} %d\n", nodeID, v)
		})
		writeHelpType(&b, "kladovka_cluster_node_used_bytes", "gauge", "Used bytes reported by cluster node")
		forEachCounter(&clusterUsed, func(nodeID string, v uint64) {
			fmt.Fprintf(&b, "kladovka_cluster_node_used_bytes{node_id=%q} %d\n", nodeID, v)
		})

		writeHelpType(&b, "kladovka_replication_jobs_published_total", "counter", "Replication jobs published to peers")
		fmt.Fprintf(&b, "kladovka_replication_jobs_published_total %d\n", replPublished.Load())
		writeHelpType(&b, "kladovka_replication_ok_total", "counter", "Successful replication copies")
		fmt.Fprintf(&b, "kladovka_replication_ok_total %d\n", replOK.Load())
		writeHelpType(&b, "kladovka_replication_failed_total", "counter", "Failed replication copies")
		fmt.Fprintf(&b, "kladovka_replication_failed_total %d\n", replFailed.Load())

		_, _ = w.Write([]byte(b.String()))
	})
}

func writeHelpType(b *strings.Builder, name, typ, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s %s\n", name, typ)
}

func forEachCounter(m *sync.Map, fn func(key string, v uint64)) {
	type kv struct {
		k string
		v uint64
	}
	var items []kv
	m.Range(func(key, value any) bool {
		items = append(items, kv{key.(string), value.(*atomic.Uint64).Load()})
		return true
	})
	sort.Slice(items, func(i, j int) bool { return items[i].k < items[j].k })
	for _, it := range items {
		fn(it.k, it.v)
	}
}
