// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/rkislov/kladovka/internal/testprofile"
)

// One iteration is three rounds of four concurrent unique 8MiB PUTs through the Go backend.
// HTTP, SDK and signing are covered separately by workload_pilot.py.
func BenchmarkProfileConcurrentPUT(b *testing.B) {
	f := newFixture(b)
	s := f.nodes[f.leader(b)]
	if err := s.CreatePolka("profile"); err != nil {
		b.Fatal(err)
	}
	bodies := make([][]byte, 4)
	for i := range bodies {
		bodies[i] = make([]byte, 8<<20)
		rand.New(rand.NewSource(int64(i + 1))).Read(bodies[i])
	}
	stop := testprofile.Start(b, "consensus")
	defer stop()
	b.ResetTimer()
	b.SetBytes(3 * 4 * 8 << 20)
	b.ReportAllocs()
	for round := 0; round < 3*b.N; round++ {
		var wg sync.WaitGroup
		errors := make(chan error, 4)
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				// Make every 256KiB chunk unique even across benchmark rounds.
				for offset := 0; offset < len(bodies[worker]); offset += ChunkSize {
					bodies[worker][offset] = byte(round)
					bodies[worker][offset+1] = byte(round >> 8)
				}
				_, err := s.PutObject("profile", fmt.Sprintf("%d-%d", round, worker), bytes.NewReader(bodies[worker]), "application/octet-stream")
				errors <- err
			}(worker)
		}
		wg.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

// A larger 1MiB C4 batch crosses snapshot intervals on the local APFS fixture.
// Use -benchtime=1x with opt-in profiles; memstats describe the entire test process.
func BenchmarkProfileSustainedPUT(b *testing.B) {
	f := newFixture(b)
	s := f.nodes[f.leader(b)]
	if err := s.CreatePolka("profile"); err != nil {
		b.Fatal(err)
	}
	bodies := make([][]byte, 4)
	for i := range bodies {
		bodies[i] = make([]byte, 1<<20)
		rand.New(rand.NewSource(int64(i + 1))).Read(bodies[i])
	}
	stop := testprofile.Start(b, "sustained")
	defer stop()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	b.SetBytes(256 << 20)
	b.ReportAllocs()
	for round := 0; round < 64*b.N; round++ {
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for offset := 0; offset < len(bodies[w]); offset += ChunkSize {
					bodies[w][offset] = byte(round)
					bodies[w][offset+1] = byte(round >> 8)
				}
				_, err := s.PutObject("profile", fmt.Sprintf("%d-%d", round, w), bytes.NewReader(bodies[w]), "application/octet-stream")
				errs <- err
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)
	b.Logf("heap_alloc=%d heap_inuse=%d heap_sys=%d total_alloc_delta=%d gc_delta=%d pause_ns_delta=%d", after.HeapAlloc, after.HeapInuse, after.HeapSys, after.TotalAlloc-before.TotalAlloc, after.NumGC-before.NumGC, after.PauseTotalNs-before.PauseTotalNs)
	for i, node := range f.nodes {
		b.Logf("node %d bolt stats: %+v", i, node.logs.Stats())
	}
}
