// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metrics

import (
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/rkislov/kladovka/internal/cluster"
)

// PrometheusSDTarget is one HTTP SD entry for Prometheus.
type PrometheusSDTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// SDHandler returns Prometheus HTTP service discovery JSON for cluster nodes.
func SDHandler(reg *cluster.Registry, metricsPath string) http.Handler {
	if metricsPath == "" {
		metricsPath = "/metrics"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var out []PrometheusSDTarget
		for _, n := range reg.Snapshot() {
			hostPort, scheme := hostPortScheme(n.URL)
			if hostPort == "" {
				continue
			}
			out = append(out, PrometheusSDTarget{
				Targets: []string{hostPort},
				Labels: map[string]string{
					"node_id":         n.NodeID,
					"metrics_path":    metricsPath,
					"metrics_scheme":  scheme,
					"__metrics_path__": metricsPath,
					"__scheme__":      scheme,
				},
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
}

func hostPortScheme(raw string) (hostPort, scheme string) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		// bare host:port
		if strings.Contains(raw, "://") {
			return "", "http"
		}
		return strings.TrimRight(raw, "/"), "http"
	}
	scheme = u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(host, port), scheme
}
