// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package storage

import (
	"strings"
	"testing"
)

func TestPaginationExactBoundary(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	s.PutObject("demo", "a", strings.NewReader("a"), "")
	v, truncated, e := s.ListObjects("demo", "", 1, "")
	if e != nil || len(v) != 1 || truncated {
		t.Fatalf("%v %v %v", v, truncated, e)
	}
}
func TestMultipartRejectsWrongTargetAndETag(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	id, _ := s.CreateMultipartUpload("demo", "key", "")
	part, e := s.UploadPart(id, 1, strings.NewReader("hello"))
	if e != nil {
		t.Fatal(e)
	}
	if s.ValidateMultipartTarget(id, "demo", "other") == nil {
		t.Fatal("target mismatch accepted")
	}
	wrong := *part
	wrong.ETag = `"wrong"`
	if _, e = s.CompleteMultipartUpload(id, []MultipartPart{wrong}); e == nil {
		t.Fatal("bad ETag accepted")
	}
	if _, e = s.ListParts("../outside"); e == nil {
		t.Fatal("invalid upload id accepted")
	}
	meta, e := s.CompleteMultipartUpload(id, []MultipartPart{*part})
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasSuffix(meta.ETag, `-1"`) {
		t.Fatal("not a multipart ETag", meta.ETag)
	}
	listed, _, err := s.ListObjects("demo", "", 100, "")
	if err != nil || len(listed) != 1 || listed[0].ETag != meta.ETag {
		t.Fatal("multipart catalogue differs", listed, err)
	}
}
