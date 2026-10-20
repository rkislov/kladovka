// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// This helper intentionally exits without deferred cleanup, as a crashed writer does.
func TestCrashWriterProcess(t *testing.T) {
	root := os.Getenv("KLADOVKA_CRASH_TEST_ROOT")
	if root == "" {
		return
	}
	s, err := New(root)
	if err != nil {
		os.Exit(90)
	}
	s.afterStep = func(step string) error {
		if step == os.Getenv("KLADOVKA_CRASH_TEST_STEP") {
			os.Exit(91)
		}
		return nil
	}
	if os.Getenv("KLADOVKA_CRASH_TEST_MODE") == "delete" {
		s.DeleteObject("demo", "nested/file")
	} else {
		s.PutObject("demo", "nested/file", strings.NewReader("new durable value"), "text/plain")
	}
	os.Exit(92)
}
func TestRecoverAfterWriterCrash(t *testing.T) {
	for _, mode := range []string{"put", "delete"} {
		for _, step := range []string{"prepared", "data-published", "metadata-published"} {
			t.Run(mode+"/"+step, func(t *testing.T) {
				root := t.TempDir()
				s, err := New(root)
				if err != nil {
					t.Fatal(err)
				}
				s.CreatePolka("demo")
				if _, err = s.PutObject("demo", "nested/file", strings.NewReader("old value"), "text/plain"); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWriterProcess$")
				cmd.Env = append(os.Environ(), "KLADOVKA_CRASH_TEST_ROOT="+root, "KLADOVKA_CRASH_TEST_MODE="+mode, "KLADOVKA_CRASH_TEST_STEP="+step)
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 91 {
					t.Fatalf("helper did not reach crash point: %v %s", err, out)
				}
				restored, err := New(root)
				if err != nil {
					t.Fatal(err)
				}
				rc, meta, err := restored.GetObject("demo", "nested/file")
				if mode == "delete" {
					if !errors.Is(err, ErrObjectNotFound) {
						t.Fatalf("delete recovery: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					b, _ := io.ReadAll(rc)
					rc.Close()
					sum := sha256.Sum256(b)
					if string(b) != "new durable value" || meta.SHA256 != hex.EncodeToString(sum[:]) {
						t.Fatalf("invalid recovered pair: %q %+v", b, meta)
					}
				}
				pending, _ := os.ReadDir(restored.transactionRoot())
				if len(pending) != 0 {
					t.Fatal("journal not drained")
				}
			})
		}
	}
}
func TestFailedCommitIsNotReadable(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	s.PutObject("demo", "key", strings.NewReader("old"), "")
	s.afterStep = func(step string) error {
		if step == "data-published" {
			return errors.New("metadata volume failure")
		}
		return nil
	}
	if _, err := s.PutObject("demo", "key", strings.NewReader("new"), ""); err == nil {
		t.Fatal("write succeeded")
	}
	if _, _, err := s.GetObject("demo", "key"); !errors.Is(err, ErrPendingWrite) {
		t.Fatal("unfinished pair exposed", err)
	}
	restored, err := New(s.root)
	if err != nil {
		t.Fatal(err)
	}
	rc, _, err := restored.GetObject("demo", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if string(b) != "new" {
		t.Fatal(string(b))
	}
}

type brokenReader struct{ first bool }

func (r *brokenReader) Read(b []byte) (int, error) {
	if !r.first {
		r.first = true
		return copy(b, "half"), nil
	}
	return 0, errors.New("source disconnected")
}
func TestFailedInputPreservesOldObject(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	s.PutObject("demo", "key", strings.NewReader("safe"), "")
	if _, e := s.PutObject("demo", "key", &brokenReader{}, ""); e == nil {
		t.Fatal("broken source accepted")
	}
	rc, _, e := s.GetObject("demo", "key")
	if e != nil {
		t.Fatal(e)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if string(b) != "safe" {
		t.Fatal(string(b))
	}
}
func TestReadRejectsCorruption(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	s.PutObject("demo", "key", strings.NewReader("hello"), "")
	op, _ := s.objectPath("demo", "key")
	os.WriteFile(op, []byte("jello"), 0600)
	if _, _, err := s.GetObject("demo", "key"); !errors.Is(err, ErrIntegrity) {
		t.Fatal("corrupt bytes accepted", err)
	}
}
func TestQuotaIncludesMultipartAndDoesNotPublishPartialData(t *testing.T) {
	s, err := NewWithOptions(t.TempDir(), Options{QuotaBytes: 10, VerifyReads: true})
	if err != nil {
		t.Fatal(err)
	}
	s.CreatePolka("demo")
	s.PutObject("demo", "old", strings.NewReader("1234"), "")
	if _, err = s.PutObject("demo", ".transactions/bypass", strings.NewReader("1234567"), ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("quota bypass", err)
	}
	id, _ := s.CreateMultipartUpload("demo", "multi", "")
	if _, err = s.UploadPart(id, 1, strings.NewReader("123456")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutObject("demo", "new", strings.NewReader("x"), ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("parts not counted", err)
	}
	if _, _, err = s.GetObject("demo", "new"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatal("partial object exists", err)
	}
	if err = s.AbortMultipartUpload(id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutObject("demo", "new", strings.NewReader("123456"), ""); err != nil {
		t.Fatal("quota not freed", err)
	}
}
func TestDiskReserveRejectsWrite(t *testing.T) {
	s, _ := NewWithOptions(t.TempDir(), Options{MinFreeBytes: ^uint64(0)})
	s.CreatePolka("demo")
	if _, err := s.PutObject("demo", "key", strings.NewReader("x"), ""); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatal(err)
	}
}
func TestCleanupSkipsPinnedAndRecentMultipart(t *testing.T) {
	s, _ := NewWithOptions(t.TempDir(), Options{MultipartTTL: time.Hour})
	s.CreatePolka("demo")
	old, _ := s.CreateMultipartUpload("demo", "old", "")
	recent, _ := s.CreateMultipartUpload("demo", "recent", "")
	past := time.Now().Add(-2 * time.Hour)
	filepath.WalkDir(s.multipartDir(old), func(path string, d fs.DirEntry, e error) error {
		if e == nil {
			os.Chtimes(path, past, past)
		}
		return e
	})
	release, err := s.PinMultipart(old)
	if err != nil {
		t.Fatal(err)
	}
	if n, e := s.CleanupMultipart(time.Now()); e != nil || n != 0 {
		t.Fatalf("active upload removed: %d %v", n, e)
	}
	release()
	if n, e := s.CleanupMultipart(time.Now()); e != nil || n != 1 {
		t.Fatalf("cleanup: %d %v", n, e)
	}
	if _, e := s.ListParts(recent); e != nil {
		t.Fatal("recent upload removed", e)
	}
}
func TestConcurrentLocalUpdatesRemainConsistent(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	var wg sync.WaitGroup
	problems := make(chan error, 20)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if _, e := s.PutObject("demo", "key", strings.NewReader(strings.Repeat("content", j+1)), "text/plain"); e != nil {
					problems <- e
					return
				}
				rc, _, e := s.GetObject("demo", "key")
				if e != nil && !errors.Is(e, ErrObjectNotFound) {
					problems <- e
					return
				}
				if rc != nil {
					rc.Close()
				}
				if e = s.DeleteObject("demo", "key"); e != nil && !errors.Is(e, ErrObjectNotFound) {
					problems <- e
					return
				}
			}
		}()
	}
	wg.Wait()
	close(problems)
	for e := range problems {
		t.Error(e)
	}
}
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return os.WriteFile(target, b, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestBackupRestoreIncludesMetadataAndMultipart(t *testing.T) {
	s, _ := New(t.TempDir())
	s.CreatePolka("demo")
	s.PutObject("demo", "nested/key", strings.NewReader("backup data"), "text/plain")
	id, _ := s.CreateMultipartUpload("demo", "resume", "")
	part, err := s.UploadPart(id, 1, strings.NewReader("resumable"))
	if err != nil {
		t.Fatal(err)
	}
	backup := t.TempDir()
	copyTree(t, s.root, backup)
	restored, e := New(backup)
	if e != nil {
		t.Fatal(e)
	}
	rc, meta, e := restored.GetObject("demo", "nested/key")
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "backup data" || meta.SHA256 == "" || meta.ContentType != "text/plain" {
		t.Fatal("backup not intact")
	}
	if _, e = restored.CompleteMultipartUpload(id, []MultipartPart{*part}); e != nil {
		t.Fatal("multipart did not survive backup", e)
	}
}

func TestLegacyUpgradeAnchorsChecksumAndThenRequiresMetadata(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "demo"), 0700)
	object := filepath.Join(root, "demo", "old")
	os.WriteFile(object, []byte("legacy bytes"), 0600)
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rc, meta, err := s.GetObject("demo", "old")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if len(meta.SHA256) != 64 {
		t.Fatal("legacy checksum missing")
	}
	if _, err = os.Stat(filepath.Join(root, formatName)); err != nil {
		t.Fatal("format marker missing", err)
	}
	os.Remove(object + ".kladovka-meta.json")
	if _, _, err = s.GetObject("demo", "old"); !errors.Is(err, ErrIntegrity) {
		t.Fatal("missing metadata silently accepted", err)
	}
}
func TestUnsupportedStorageFormatIsRejected(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, formatName), []byte(`{"version":999}`), 0600)
	if _, err := New(root); err == nil {
		t.Fatal("future storage format accepted")
	}
}
