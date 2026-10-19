// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package s3api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/auth"
	"github.com/rkislov/kladovka/internal/cluster"
	"github.com/rkislov/kladovka/internal/config"
	"github.com/rkislov/kladovka/internal/storage"
)

func testServer(t *testing.T, limit int64) (*Server, http.Handler) {
	t.Helper()
	st, e := storage.New(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	cfg := &config.Config{AccessKey: "test", SecretKey: "secret", ClusterToken: "cluster-secret", Region: "us-east-1", NodeID: "node", NodeURL: "http://localhost", ReplicationFactor: 1, MaxObjectSize: limit}
	reg := cluster.NewRegistry(cfg.NodeID, cfg.NodeURL, nil, true, time.Second)
	reg.SetToken(cfg.ClusterToken)
	s := &Server{Cfg: cfg, Store: st, Reg: reg, Auth: &auth.Authenticator{Region: cfg.Region, Creds: auth.Credentials{cfg.AccessKey: cfg.SecretKey}}, Rep: &cluster.Replicator{Registry: reg, ReplicationFactor: 1}}
	return s, s.Handler()
}
func call(h http.Handler, method, path string, body []byte, credential string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	if credential == "basic" {
		r.SetBasicAuth("test", "secret")
	} else if credential != "" {
		r.Header.Set("Authorization", credential)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func sign(r *http.Request, body []byte, when time.Time) {
	sum := sha256.Sum256(body)
	payload := hex.EncodeToString(sum[:])
	r.Header.Set("X-Amz-Content-Sha256", payload)
	stamp := when.UTC().Format("20060102T150405Z")
	date := when.UTC().Format("20060102")
	r.Header.Set("X-Amz-Date", stamp)
	q := r.URL.Query().Encode()
	q = strings.ReplaceAll(q, "+", "%20")
	canonical := strings.Join([]string{r.Method, r.URL.EscapedPath(), q, "host:" + r.Host + "\nx-amz-content-sha256:" + payload + "\nx-amz-date:" + stamp + "\n", "host;x-amz-content-sha256;x-amz-date", payload}, "\n")
	hash := sha256.Sum256([]byte(canonical))
	scope := date + "/us-east-1/s3/aws4_request"
	message := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(hash[:])
	mac := func(k []byte, v string) []byte { m := hmac.New(sha256.New, k); m.Write([]byte(v)); return m.Sum(nil) }
	key := mac([]byte("AWS4secret"), date)
	for _, v := range []string{"us-east-1", "s3", "aws4_request"} {
		key = mac(key, v)
	}
	signature := hex.EncodeToString(mac(key, message))
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=test/%s, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s", scope, signature))
}
func TestProtectedEndpointsAndCSRF(t *testing.T) {
	_, h := testServer(t, 100)
	for _, path := range []string{"/ui/", "/api/polki", "/api/stats", "/metrics", "/cluster", "/internal/health", "/internal/object/demo/x"} {
		if w := call(h, "GET", path, nil, ""); w.Code != 401 {
			t.Errorf("%s: %d", path, w.Code)
		}
	}
	if w := call(h, "GET", "/healthz", nil, ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	r := httptest.NewRequest("POST", "/api/polki", strings.NewReader(`{"name":"demo"}`))
	r.SetBasicAuth("test", "secret")
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}
func TestObjectWorkflowRangeLimitsAndCleanup(t *testing.T) {
	s, h := testServer(t, 5)
	if w := call(h, "POST", "/api/polki", []byte(`{"name":"demo"}`), "basic"); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(h, "PUT", "/api/object/demo/folder/test", []byte("hello"), "basic"); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	r := httptest.NewRequest("GET", "/api/object/demo/folder/test", nil)
	r.SetBasicAuth("test", "secret")
	r.Header.Set("Range", "bytes=0-1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 206 || w.Body.String() != "he" {
		t.Fatalf("range: %d %q", w.Code, w.Body.String())
	}
	for _, credential := range []string{"basic", "Bearer cluster-secret"} {
		path := "/api/object/demo/large"
		if credential != "basic" {
			path = "/internal/object/demo/large"
		}
		w := call(h, "PUT", path, []byte("123456"), credential)
		if w.Code != 413 {
			t.Fatalf("limit: %d %s", w.Code, w.Body.String())
		}
	}
	if w := call(h, "DELETE", "/api/object/demo/folder/test", nil, "basic"); w.Code != 204 {
		t.Fatal(w.Code)
	}
	if w := call(h, "DELETE", "/api/polki/demo", nil, "basic"); w.Code != 204 {
		t.Fatal(w.Body.String())
	}
	exists, _ := s.Store.PolkaExists("demo")
	if exists {
		t.Fatal("shelf remains")
	}
}
func TestSigV4MultipartAndReplay(t *testing.T) {
	_, h := testServer(t, 64)
	call(h, "POST", "/api/polki", []byte(`{"name":"demo"}`), "basic")
	r := httptest.NewRequest("POST", "/demo/key?uploads", nil)
	sign(r, nil, time.Now())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "UploadId") {
		t.Fatalf("multipart %d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest("PUT", "/demo/replay", strings.NewReader("hello"))
	sign(r, []byte("hello"), time.Now().Add(-time.Hour))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("old signature accepted", w.Code)
	}
	r = httptest.NewRequest("PUT", "/demo/corrupt", strings.NewReader("wrong"))
	sign(r, []byte("hello"), time.Now())
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("payload mismatch accepted", w.Code)
	}
}
func TestShelfPaginationAndCapacity(t *testing.T) {
	s, h := testServer(t, 100)
	s.Store.CreatePolka("demo")
	for i := 0; i < 101; i++ {
		if _, e := s.Store.PutObject("demo", fmt.Sprintf("%03d", i), strings.NewReader("x"), ""); e != nil {
			t.Fatal(e)
		}
	}
	w := call(h, "GET", "/api/polki/demo", nil, "basic")
	var j struct {
		Objects   []storage.ObjectMeta `json:"objects"`
		Truncated bool                 `json:"truncated"`
		Next      string               `json:"next_start_after"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &j); e != nil {
		t.Fatal(e)
	}
	if len(j.Objects) != 100 || !j.Truncated || j.Next != "099" {
		t.Fatalf("pagination: %+v", j)
	}
	w = call(h, "GET", "/api/polki/demo?start-after="+j.Next, nil, "basic")
	json.Unmarshal(w.Body.Bytes(), &j)
	if len(j.Objects) != 1 || j.Truncated {
		t.Fatalf("last page: %+v", j)
	}
}

func TestTwoNodeForwardingReplicationAndFailure(t *testing.T) {
	servers := []*httptest.Server{httptest.NewUnstartedServer(nil), httptest.NewUnstartedServer(nil)}
	nodes := make([]*Server, 2)
	for i, ts := range servers {
		self := "http://" + ts.Listener.Addr().String()
		peer := "http://" + servers[1-i].Listener.Addr().String()
		st, err := storage.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		cfg := &config.Config{AccessKey: "test", SecretKey: "secret", Region: "us-east-1", NodeID: cluster.NodeIDFromURL(self), NodeURL: self, ReplicationFactor: 2, ClusterToken: "cluster-secret", MaxConcurrentUploads: 1}
		reg := cluster.NewRegistry(cfg.NodeID, self, []string{peer}, true, time.Second)
		reg.SetToken(cfg.ClusterToken)
		s := &Server{Cfg: cfg, Store: st, Reg: reg, Rep: &cluster.Replicator{Registry: reg, ReplicationFactor: 2}, Auth: &auth.Authenticator{Region: cfg.Region, Creds: auth.Credentials{"test": "secret"}}}
		nodes[i] = s
		ts.Config.Handler = s.Handler()
		ts.Start()
		t.Cleanup(ts.Close)
	}
	do := func(i int, method, path string, body []byte, signed bool) *http.Response {
		t.Helper()
		r, e := http.NewRequest(method, servers[i].URL+path, bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		if signed {
			sign(r, body, time.Now())
		} else {
			r.SetBasicAuth("test", "secret")
		}
		resp, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		return resp
	}
	resp := do(0, "POST", "/api/polki", []byte(`{"name":"demo"}`), false)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatal("create", resp.StatusCode)
	}
	for i := range servers {
		resp = do(i, "PUT", "/demo/nested/key", []byte("hello"), true)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("forward %d: %d %s", i, resp.StatusCode, b)
		}
	}
	for _, s := range nodes {
		meta, e := s.Store.HeadObject("demo", "nested/key")
		if e != nil || meta.Size != 5 {
			t.Fatalf("replica: %+v %v", meta, e)
		}
	}
	// Force a local miss and verify range fallback through the authenticated internal API.
	if e := nodes[0].Store.DeleteObject("demo", "nested/key"); e != nil {
		t.Fatal(e)
	}
	r, _ := http.NewRequest("GET", servers[0].URL+"/api/object/demo/nested/key", nil)
	r.SetBasicAuth("test", "secret")
	r.Header.Set("Range", "bytes=1-2")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 206 || string(b) != "el" {
		t.Fatalf("fallback range %d %q", resp.StatusCode, b)
	}
	// Failed replication must never be reported as a successful write.
	primary := 0
	if !nodes[0].Rep.IsPrimary("demo", "offline") {
		primary = 1
	}
	servers[1-primary].Close()
	resp = do(primary, "PUT", "/demo/offline", []byte("hello"), true)
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal("failed replication returned", resp.StatusCode)
	}
}

func TestMultipartHTTPCompletionAndTargetBinding(t *testing.T) {
	s, h := testServer(t, 1024)
	s.Store.CreatePolka("demo")
	w := call(h, "POST", "/api/object/demo/final?uploads", nil, "basic")
	var initiated struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &initiated); err != nil || initiated.ID == "" {
		t.Fatalf("init %d %s", w.Code, w.Body.String())
	}
	path := "/api/object/demo/final?uploadId=" + initiated.ID
	w = call(h, "PUT", path+"&partNumber=1", []byte("hello"), "basic")
	if w.Code != 200 {
		t.Fatalf("part %d %s", w.Code, w.Body.String())
	}
	etag := w.Header().Get("ETag")
	wrong := call(h, "GET", "/api/object/demo/another?uploadId="+initiated.ID, nil, "basic")
	if wrong.Code != 404 {
		t.Fatal("wrong target", wrong.Code)
	}
	body := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + etag + "</ETag></Part></CompleteMultipartUpload>")
	w = call(h, "POST", path, body, "basic")
	if w.Code != 200 {
		t.Fatalf("complete %d %s", w.Code, w.Body.String())
	}
	w = call(h, "GET", "/api/object/demo/final", nil, "basic")
	if w.Body.String() != "hello" || !strings.HasSuffix(w.Header().Get("ETag"), `-1"`) {
		t.Fatalf("final %d %s %s", w.Code, w.Body.String(), w.Header().Get("ETag"))
	}
	if _, e := s.Store.ListParts(initiated.ID); !errors.Is(e, storage.ErrNoSuchUpload) {
		t.Fatal("parts not cleaned", e)
	}
}

func TestMultipartCanRetryAfterReplicationFailure(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer cluster-secret" {
			w.WriteHeader(401)
			return
		}
		io.Copy(io.Discard, r.Body)
		if fail.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	defer replica.Close()
	s, _ := testServer(t, 1024)
	s.Cfg.ReplicationFactor = 2
	s.Reg = cluster.NewRegistry("node", "http://localhost", []string{replica.URL}, true, time.Second)
	s.Reg.SetToken("cluster-secret")
	s.Rep = &cluster.Replicator{Registry: s.Reg, ReplicationFactor: 2}
	key := "retry"
	for !s.Rep.IsPrimary("demo", key) {
		key += "x"
	}
	s.Store.CreatePolka("demo")
	id, e := s.Store.CreateMultipartUpload("demo", key, "")
	if e != nil {
		t.Fatal(e)
	}
	part, e := s.Store.UploadPart(id, 1, strings.NewReader("hello"))
	if e != nil {
		t.Fatal(e)
	}
	body := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + part.ETag + "</ETag></Part></CompleteMultipartUpload>")
	path := "/api/object/demo/" + key + "?uploadId=" + id
	w := call(s.Handler(), "POST", path, body, "basic")
	if w.Code != 503 {
		t.Fatalf("expected failure: %d %s", w.Code, w.Body.String())
	}
	if _, e = s.Store.ListParts(id); e != nil {
		t.Fatal("retry source lost", e)
	}
	fail.Store(false)
	w = call(s.Handler(), "POST", path, body, "basic")
	if w.Code != 200 {
		t.Fatalf("retry: %d %s", w.Code, w.Body.String())
	}
	if _, e = s.Store.ListParts(id); !errors.Is(e, storage.ErrNoSuchUpload) {
		t.Fatal("retry cleanup", e)
	}
}
