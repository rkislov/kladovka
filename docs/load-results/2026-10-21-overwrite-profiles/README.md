# Fixed-key retained-heap diagnostics

Production code: 3b1369f. Go 1.25.0, Apple M4, macOS 27.0.1/APFS.

Command: `KLADOVKA_OVERWRITE_EPOCHS=32 KLADOVKA_BENCH_PROFILE_DIR=/tmp/kladovka-overwrite-heap-profile go test ./internal/consensus -run '^$' -bench '^BenchmarkProfileOverwriteHeap$' -benchtime=1x -count=1 -v`.

Three Raft nodes in one test process, 16 keys × 1 MiB, 32 epochs × 32 PUT, C4. Explicit prune and GC after each epoch. CPU/block/mutex/allocs cover the test including fixture preparation; final heap is sampled after an additional GC, before fixture cleanup. These profiles diagnose retained memory, not HTTP throughput. Benchmark log and full methodology are in the parent directory and LOAD_PILOT.md.
