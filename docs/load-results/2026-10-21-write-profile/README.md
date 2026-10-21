# Профили записи · 21 октября 2026

- before: исходный код сервера d8a7dd0; тестовый harness 48dcb93.
- after: код сервера dee728d (при снятии профилей ещё не закоммичен, затем зафиксирован без изменения реализации).
- Apple M4, 24 ГиБ, Go 1.25.0, macOS APFS; CPU, block и mutex включены только на измеряемый пакет. allocs накоплен за жизнь тестового процесса, включая fixture.
- storage: 50 overwrite+LIST по 1000 ключам.
- consensus: 12 уникальных PUT по 8 МиБ, C4, три Raft-узла внутри одного процесса.
- В *.benchmark.txt сохранены исходные B/op, allocs/op и ns/op всего пакета. MB/s — десятичные.
- Отчёт и ограничения: [LOAD_PILOT.md](../../LOAD_PILOT.md).

Чтение не требует тестового бинарника для символьного top:

```sh
go tool pprof -top docs/load-results/2026-10-21-write-profile/before/storage.cpu.pprof
go tool pprof -top docs/load-results/2026-10-21-write-profile/after/consensus.allocs.pprof
go tool pprof -top docs/load-results/2026-10-21-write-profile/after/consensus.block.pprof
```

Для построчного анализа используйте исходники соответствующего коммита. Имена файлов/стеков включены в pprof, тела объектов и credentials не записывались.
