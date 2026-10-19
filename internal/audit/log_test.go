// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").
package audit

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestConcurrentAuditAndRestartTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	if err := os.WriteFile(path, []byte(`{"interrupted":`), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Append(Event{Kind: "result", Actor: "test"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	l.Close()
	data, _ := os.ReadFile(path)
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 17 {
		t.Fatal("incomplete records")
	}
	for _, line := range lines[1:] {
		if !json.Valid(line) {
			t.Fatal("interleaved JSON")
		}
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(path); err == nil {
		l.Close()
		t.Fatal("public audit log accepted")
	}
}
