// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// NodeState is observable health of one cluster member.
type NodeState struct {
	NodeID    string `json:"node_id"`
	URL       string `json:"url"`
	Healthy   bool   `json:"healthy"`
	UsedBytes uint64 `json:"used_bytes"`
	LastError string `json:"last_error,omitempty"`
}

// Registry tracks self + peers and polls health.
type Registry struct {
	mu       sync.RWMutex
	selfID   string
	selfURL  string
	nodes    map[string]*NodeState
	client   *http.Client
	interval time.Duration
}

// NewRegistry builds a registry from self and peer base URLs.
func NewRegistry(selfID, selfURL string, peerURLs []string, verifyTLS bool, interval time.Duration) *Registry {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !verifyTLS}, //nolint:gosec // configurable for lab TLS
	}
	r := &Registry{
		selfID:   selfID,
		selfURL:  selfURL,
		nodes:    map[string]*NodeState{},
		client:   &http.Client{Timeout: 3 * time.Second, Transport: tr},
		interval: interval,
	}
	r.nodes[selfID] = &NodeState{NodeID: selfID, URL: selfURL, Healthy: true}
	for _, u := range peerURLs {
		id := NodeIDFromURL(u)
		if id == selfID {
			continue
		}
		r.nodes[id] = &NodeState{NodeID: id, URL: u, Healthy: false}
	}
	return r
}

// NodeIDFromURL derives a stable id host:port from a base URL.
func NodeIDFromURL(raw string) string {
	// Keep simple: strip scheme and trailing slash.
	u := raw
	for _, p := range []string{"https://", "http://"} {
		if len(u) > len(p) && u[:len(p)] == p {
			u = u[len(p):]
			break
		}
	}
	for len(u) > 0 && u[len(u)-1] == '/' {
		u = u[:len(u)-1]
	}
	if u == "" {
		return raw
	}
	return u
}

// SortedNodeIDs returns stable order for consistent hashing.
func (r *Registry) SortedNodeIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// GetURL returns base URL for a node id.
func (r *Registry) GetURL(nodeID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n := r.nodes[nodeID]; n != nil {
		return n.URL
	}
	return ""
}

// SelfID returns this node's id.
func (r *Registry) SelfID() string { return r.selfID }

// Snapshot returns a copy of all node states.
func (r *Registry) Snapshot() []NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeState, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// SetSelfUsed updates local used bytes.
func (r *Registry) SetSelfUsed(used uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := r.nodes[r.selfID]; n != nil {
		n.UsedBytes = used
		n.Healthy = true
		n.LastError = ""
	}
}

// StartHealthLoop periodically pings peers until ctx is cancelled.
func (r *Registry) StartHealthLoop(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	r.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.pollOnce(ctx)
		}
	}
}

func (r *Registry) pollOnce(ctx context.Context) {
	r.mu.RLock()
	peers := make([]*NodeState, 0, len(r.nodes))
	for _, n := range r.nodes {
		if n.NodeID != r.selfID {
			peers = append(peers, n)
		}
	}
	r.mu.RUnlock()

	for _, n := range peers {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.URL+"/internal/health", nil)
		if err != nil {
			r.mark(n.NodeID, false, 0, err.Error())
			continue
		}
		resp, err := r.client.Do(req)
		if err != nil {
			r.mark(n.NodeID, false, 0, err.Error())
			continue
		}
		var body struct {
			UsedBytes uint64 `json:"used_bytes"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			r.mark(n.NodeID, false, 0, "health "+resp.Status)
			continue
		}
		r.mark(n.NodeID, true, body.UsedBytes, "")
	}
}

func (r *Registry) mark(id string, healthy bool, used uint64, lastErr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := r.nodes[id]; n != nil {
		n.Healthy = healthy
		n.UsedBytes = used
		n.LastError = lastErr
	}
}

// Client returns the shared HTTP client for inter-node calls.
func (r *Registry) Client() *http.Client { return r.client }
