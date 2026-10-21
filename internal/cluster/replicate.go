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
	"encoding/json"
	"fmt"
	"github.com/rkislov/kladovka/internal/storage"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Replicator copies objects to peer nodes via internal API.
type Replicator struct {
	Registry          *Registry
	ReplicationFactor int
}

// IsPrimary reports whether this node is primary for the object.
func (r *Replicator) IsPrimary(polka, key string) bool {
	nodes := NodesForObject(polka, key, r.Registry.SortedNodeIDs(), r.ReplicationFactor)
	if len(nodes) == 0 {
		return true
	}
	return nodes[0] == r.Registry.SelfID()
}

// Placement returns ordered node ids for an object.
func (r *Replicator) Placement(polka, key string) []string {
	return NodesForObject(polka, key, r.Registry.SortedNodeIDs(), r.ReplicationFactor)
}

// PrimaryURL returns the primary node's base URL for an object.
func (r *Replicator) PrimaryURL(polka, key string) string {
	nodes := r.Placement(polka, key)
	if len(nodes) == 0 {
		return r.Registry.selfURL
	}
	return r.Registry.GetURL(nodes[0])
}

// ReplicatePut sends object bytes to replica peers (not self).
func (r *Replicator) ReplicatePut(ctx context.Context, polka, key string, body io.ReadSeeker, contentType string, etag string, versions ...*storage.ObjectMeta) error {
	nodes := r.Placement(polka, key)
	self := r.Registry.SelfID()
	var firstErr error
	for _, nid := range nodes {
		if nid == self {
			continue
		}
		base := r.Registry.GetURL(nid)
		if base == "" {
			continue
		}
		u := fmt.Sprintf("%s/internal/object/%s/%s", base, url.PathEscape(polka), escapeKey(key))
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, io.NopCloser(body))
		if err != nil {
			firstErr = err
			continue
		}
		if etag != "" {
			req.Header.Set("X-Kladovka-ETag", etag)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if len(versions) > 0 && versions[0] != nil && versions[0].VersionID != "" {
			b, _ := json.Marshal(versions[0])
			req.Header.Set("X-Kladovka-Version-Meta", string(b))
		}
		r.Registry.Authorize(req)
		resp, err := r.Registry.Client().Do(req)
		if err != nil {
			firstErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			firstErr = fmt.Errorf("replicate to %s: %s", nid, resp.Status)
		}
	}
	return firstErr
}

// ReplicateDelete best-effort deletes object on peers.
func (r *Replicator) ReplicateDelete(ctx context.Context, polka, key string, versions ...*storage.ObjectMeta) error {
	var firstErr error
	nodes := r.Placement(polka, key)
	self := r.Registry.SelfID()
	for _, nid := range nodes {
		if nid == self {
			continue
		}
		base := r.Registry.GetURL(nid)
		if base == "" {
			continue
		}
		u := fmt.Sprintf("%s/internal/object/%s/%s", base, url.PathEscape(polka), escapeKey(key))
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
		if err != nil {
			firstErr = err
			continue
		}
		if len(versions) > 0 && versions[0] != nil && versions[0].VersionID != "" {
			b, _ := json.Marshal(versions[0])
			req.Header.Set("X-Kladovka-Version-Meta", string(b))
		}
		r.Registry.Authorize(req)
		resp, err := r.Registry.Client().Do(req)
		if err != nil {
			firstErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			firstErr = fmt.Errorf("replica delete: %s", resp.Status)
		}
	}
	return firstErr
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}
