// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTemporaryBudgetIsReleasedAfterFailure(t *testing.T) {
	s, h := testServer(t, 100)
	s.Cfg.MaxTempBytes = 5
	s.Store.CreatePolka("demo")
	w := call(h, "PUT", "/api/object/demo/large", []byte("123456"), "basic")
	if w.Code != 507 {
		t.Fatalf("budget %d %s", w.Code, w.Body.String())
	}
	if s.tempBytes != 0 {
		t.Fatal("reservation leaked", s.tempBytes)
	}
	w = call(h, "PUT", "/api/object/demo/fits", []byte("12345"), "basic")
	if w.Code != 200 || s.tempBytes != 0 {
		t.Fatalf("retry %d %s reserved %d", w.Code, w.Body.String(), s.tempBytes)
	}
}

type blockedBody struct {
	started chan struct{}
	release chan struct{}
	done    bool
}

func (b *blockedBody) Read(out []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	close(b.started)
	<-b.release
	b.done = true
	return copy(out, "hello"), nil
}
func (b *blockedBody) Close() error { return nil }
func TestConcurrentUploadLimitKeepsReadsAvailable(t *testing.T) {
	s, h := testServer(t, 100)
	s.Cfg.MaxConcurrentUploads = 1
	s.Store.CreatePolka("demo")
	body := &blockedBody{started: make(chan struct{}), release: make(chan struct{})}
	request := httptest.NewRequest(http.MethodPut, "/api/object/demo/first", body)
	request.SetBasicAuth("test", "secret")
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { w := httptest.NewRecorder(); h.ServeHTTP(w, request); done <- w }()
	select {
	case <-body.started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload never started")
	}
	w := call(h, "PUT", "/api/object/demo/second", []byte("hello"), "basic")
	if w.Code != 503 {
		t.Fatal("parallel limit", w.Code)
	}
	if w = call(h, "GET", "/api/stats", nil, "basic"); w.Code != 200 {
		t.Fatal("reads blocked", w.Code)
	}
	close(body.release)
	select {
	case w = <-done:
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not finish")
	}
}
