// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"github.com/rkislov/kladovka/internal/auth"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReplacementRejectsUnauthorizedAndTamperedRequests(t *testing.T) {
	for _, tc := range []struct {
		name, role, body, hash string
		code                   int
	}{
		{"reader", "reader", `{"node_id":"node","raft_id":"id"}`, "", 403},
		{"writer", "writer", `{"node_id":"node","raft_id":"id"}`, "", 403},
		{"anonymous", "", `{}`, "", 403},
		{"tampered signed body", "admin", `{"node_id":"node","raft_id":"id"}`, strings.Repeat("0", 64), 400},
		{"oversized", "admin", strings.Repeat(" ", 4097), "", 400},
		{"unknown fields", "admin", `{"force":true}`, "", 400},
		{"multiple documents", "admin", `{} {}`, "", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/cluster/replace", strings.NewReader(tc.body))
			r.Header.Set("X-Amz-Content-Sha256", tc.hash)
			if tc.role != "" {
				setPrincipal(r, auth.Principal{ID: "test", Role: tc.role})
			}
			w := httptest.NewRecorder()
			s := &Server{}
			// Consensus is deliberately absent: every rejected request must stop before
			// it can invoke the membership mutation.
			s.handleReplaceNode(w, r)
			mr := httptest.NewRequest("POST", "/api/cluster/maintenance", strings.NewReader(tc.body))
			mr.Header.Set("X-Amz-Content-Sha256", tc.hash)
			if tc.role != "" {
				setPrincipal(mr, auth.Principal{ID: "test", Role: tc.role})
			}
			mw := httptest.NewRecorder()
			s.handleMaintenance(mw, mr)
			if mw.Code != tc.code {
				t.Fatalf("maintenance status %d want %d", mw.Code, tc.code)
			}
			for _, path := range []string{"/api/cluster/add", "/api/cluster/remove"} {
				mr := httptest.NewRequest("POST", path, strings.NewReader(tc.body))
				mr.Header.Set("X-Amz-Content-Sha256", tc.hash)
				if tc.role != "" {
					setPrincipal(mr, auth.Principal{ID: "test", Role: tc.role})
				}
				mw := httptest.NewRecorder()
				s.handleMembership(mw, mr)
				if mw.Code != tc.code {
					t.Fatalf("%s status %d want %d", path, mw.Code, tc.code)
				}
			}
			if w.Code != tc.code {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.code, w.Body.String())
			}
		})
	}
}
