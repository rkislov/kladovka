# Длительная запись · исходные профили

256 уникальных PUT по 1 МиБ, 64 раунда C4, три Raft-узла внутри одного тестового процесса. Apple M4 / APFS / Go 1.25.0. HTTP-измерения лежат рядом отдельно и не включают инструменты профилирования.

- before: сервер 68efed2; harness be0ae39.
- cache: сервер c281dbb, только кеш восьми последних durable Raft logs.
- after: сервер c97688a, кеш и ограниченное параллельное сохранение чанков через ApplyBatch.
- discarded-window.benchmark.txt: проверенный, но не принятый вариант предварительного кодирования всего окна; замер 78,591 с не показал выигрыша.

Каждый каталог содержит CPU, block, mutex, allocs, heap и исходный benchmark-лог. allocs — накопленные выделения за жизнь процесса, включая fixture; B/op из лога — измеряемый пакет. heap снимается с принудительным GC после таймера и до закрытия узлов, на фоне возможной работы реплик; это не пиковый RSS. Block/mutex суммируют ожидание горутин, а не wall time. GC stats относятся ко всему тестовому процессу.

```sh
go tool pprof -top docs/load-results/2026-10-21-sustained-profiles/before/sustained.allocs.pprof
go tool pprof -top docs/load-results/2026-10-21-sustained-profiles/after/sustained.heap.pprof
KLADOVKA_BENCH_PROFILE_DIR=/tmp/new-sustained-profile go test ./internal/consensus -run '^$' -bench '^BenchmarkProfileSustainedPUT$' -benchtime=1x -count=1
```

[Методика и результаты](../../LOAD_PILOT.md).
