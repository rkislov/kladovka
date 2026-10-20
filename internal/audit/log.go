// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").
package audit

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type User struct {
	ID    string   `json:"id"`
	Role  string   `json:"role"`
	Polki []string `json:"polki"`
	Keys  []string `json:"access_keys"`
}
type Event struct {
	Time   time.Time `json:"time"`
	Node   string    `json:"node"`
	ID     string    `json:"request_id,omitempty"`
	Kind   string    `json:"kind"`
	Actor  string    `json:"actor"`
	Method string    `json:"method,omitempty"`
	Path   string    `json:"path,omitempty"`
	Status int       `json:"status,omitempty"`
	Users  []User    `json:"users,omitempty"`
}

// Log serializes JSONL records and syncs each append. The service owns one file.
type Log struct {
	mu     sync.Mutex
	file   *os.File
	broken error
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("audit log must be a private regular file (0600)")
	}
	// Separate a possibly incomplete tail after an interrupted append.
	if info.Size() > 0 {
		var b [1]byte
		if _, err = f.ReadAt(b[:], info.Size()-1); err != nil {
			f.Close()
			return nil, err
		}
		if b[0] != '\n' {
			if _, err = f.Write([]byte("\n")); err != nil {
				f.Close()
				return nil, err
			}
		}
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		f.Close()
		return nil, err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Log{file: f}, nil
}
func (l *Log) Append(e Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken != nil {
		return l.broken
	}
	e.Time = time.Now().UTC()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	record := append(b, '\n')
	n, err := l.file.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = l.file.Sync()
	}
	if err != nil {
		l.broken = err
	}
	return err
}
func (l *Log) Close() error { l.mu.Lock(); defer l.mu.Unlock(); return l.file.Close() }
