// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package storage

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"time"
)

// NewVersionID creates an opaque generation identity independent of wall clocks.
func NewVersionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type VersionBackend interface {
	ListVersions(polka, prefix string) ([]ObjectMeta, error)
	GetVersion(polka, key, id string) (io.ReadCloser, *ObjectMeta, error)
	DeleteVersion(polka, key, id string) error
	TrashObject(polka, key string) (*ObjectMeta, error)
	ExpireTrash(polka, key, expectedID string) error
}

type TrashCandidate struct {
	Polka, Key, VersionID string
	DeletedAt             time.Time
}
