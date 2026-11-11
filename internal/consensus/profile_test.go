// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"bytes"
	"fmt"
	"math/rand"
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
