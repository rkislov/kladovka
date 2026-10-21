// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
// Package testprofile records opt-in profiles for synthetic benchmarks only.
package testprofile

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
)

// Start is a no-op without KLADOVKA_BENCH_PROFILE_DIR. This package is imported
// only by _test.go files; no profiler or listener enters the service binary.
func Start(t testing.TB, name string) func() {
	t.Helper()
	dir := os.Getenv("KLADOVKA_BENCH_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, name+".cpu.pprof"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = pprof.StartCPUProfile(file); err != nil {
		file.Close()
		t.Fatal(err)
	}
	old := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	return func() {
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			t.Error(err)
		}
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(old)
		for _, kind := range []string{"block", "mutex", "allocs"} {
			f, err := os.OpenFile(filepath.Join(dir, name+"."+kind+".pprof"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Error(err)
				continue
			}
			if err = pprof.Lookup(kind).WriteTo(f, 0); err != nil {
				t.Error(err)
			}
			if err = f.Close(); err != nil {
				t.Error(err)
			}
		}
	}
}
