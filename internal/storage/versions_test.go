// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package storage

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestVersionHistoryRestoreAndCrash(t *testing.T) {
	for _, step := range []string{"", "prepared", "data-published", "metadata-published"} {
		t.Run(step, func(t *testing.T) {
			root := t.TempDir()
			s, e := NewWithOptions(root, Options{Versioning: true, VerifyReads: true})
			if e != nil {
				t.Fatal(e)
			}
			s.CreatePolka("docs")
			first, e := s.PutObject("docs", "x", strings.NewReader("first"), "text/plain")
			if e != nil {
				t.Fatal(e)
			}
			s.afterStep = func(at string) error {
				if step != "" && at == step {
					return errors.New("power loss")
				}
				return nil
			}
			_, e = s.PutObject("docs", "x", strings.NewReader("second"), "text/plain")
			if step != "" && e == nil {
				t.Fatal("fault missing")
			}
			s, e = NewWithOptions(root, Options{Versioning: true, VerifyReads: true})
			if e != nil {
				t.Fatal(e)
			}
			list, e := s.ListVersions("docs", "")
			if e != nil || len(list) != 2 {
				t.Fatalf("versions %#v %v", list, e)
			}
			r, _, e := s.GetVersion("docs", "x", first.VersionID)
			if e != nil {
				t.Fatal(e)
			}
			b, _ := io.ReadAll(r)
			r.Close()
			if string(b) != "first" {
				t.Fatal(string(b))
			}
			marker, e := s.TrashObject("docs", "x")
			if e != nil {
				t.Fatal(e)
			}
			if _, _, e = s.GetObject("docs", "x"); !errors.Is(e, ErrObjectNotFound) {
				t.Fatal(e)
			}
			if e = s.DeletePolka("docs"); !errors.Is(e, ErrPolkaNotEmpty) {
				t.Fatal("history lost", e)
			}
			r, m, e := s.GetVersion("docs", "x", first.VersionID)
			if e != nil {
				t.Fatal(e)
			}
			restored, e := s.PutObjectWithETag("docs", "x", r, m.ContentType, m.ETag)
			r.Close()
			if e != nil {
				t.Fatal(e)
			}
			if e = s.ExpireTrash("docs", "x", marker.VersionID); e != nil {
				t.Fatal(e)
			}
			r, m, e = s.GetObject("docs", "x")
			if e != nil || m.VersionID != restored.VersionID {
				t.Fatal("stale expiry deleted restored object", e)
			}
			r.Close()
			marker, e = s.TrashObject("docs", "x")
			if e != nil {
				t.Fatal(e)
			}
			if e = s.ExpireTrash("docs", "x", marker.VersionID); e != nil {
				t.Fatal(e)
			}
			list, e = s.ListVersions("docs", "")
			if e != nil || len(list) != 0 {
				t.Fatal(list, e)
			}
			if e = s.DeletePolka("docs"); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestDeleteCurrentVersionExposesPrevious(t *testing.T) {
	s, _ := NewWithOptions(t.TempDir(), Options{Versioning: true, QuotaBytes: 100})
	s.CreatePolka("docs")
	a, _ := s.PutObject("docs", "x", strings.NewReader("a"), "")
	b, _ := s.PutObject("docs", "x", strings.NewReader("b"), "")
	if e := s.DeleteVersion("docs", "x", b.VersionID); e != nil {
		t.Fatal(e)
	}
	r, m, e := s.GetObject("docs", "x")
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if m.VersionID != a.VersionID {
		t.Fatal(m)
	}
	used, _, e := s.QuotaUsage()
	if e != nil || used != 1 {
		t.Fatal(used, e)
	}
}

func TestTrashCommitRecoveryAndQuota(t *testing.T) {
	root := t.TempDir()
	s, _ := NewWithOptions(root, Options{Versioning: true, QuotaBytes: 6})
	s.CreatePolka("docs")
	first, e := s.PutObject("docs", "x", strings.NewReader("one"), "")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.PutObject("docs", "x", strings.NewReader("two"), ""); e != nil {
		t.Fatal(e)
	}
	if _, e = s.PutObject("docs", "x", strings.NewReader("3"), ""); !errors.Is(e, ErrQuotaExceeded) {
		t.Fatal("history bypassed quota", e)
	}
	s.afterStep = func(step string) error {
		if step == "data-published" {
			return errors.New("crash")
		}
		return nil
	}
	if _, e = s.TrashObject("docs", "x"); e == nil {
		t.Fatal("expected fault")
	}
	s, e = NewWithOptions(root, Options{Versioning: true, QuotaBytes: 6})
	if e != nil {
		t.Fatal(e)
	}
	versions, e := s.ListVersions("docs", "")
	if e != nil || len(versions) != 3 || !versions[0].DeleteMarker {
		t.Fatal(versions, e)
	}
	r, _, e := s.GetVersion("docs", "x", first.VersionID)
	if e != nil {
		t.Fatal(e)
	}
	r.Close()
	used, _, e := s.QuotaUsage()
	if e != nil || used != 6 {
		t.Fatal(used, e)
	}
	if e = s.ExpireTrash("docs", "x", versions[0].VersionID); e != nil {
		t.Fatal(e)
	}
	used, _, e = s.QuotaUsage()
	if e != nil || used != 0 {
		t.Fatal(used, e)
	}
}
