// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"github.com/rkislov/kladovka/internal/storage"
	"io"
)

type Backend interface {
	CreatePolka(string) error
	DeletePolka(string) error
	PolkaExists(string) (bool, error)
	ListPolki() ([]string, error)
	PutObject(string, string, io.Reader, string) (*storage.ObjectMeta, error)
	PutObjectWithETag(string, string, io.Reader, string, string) (*storage.ObjectMeta, error)
	GetObject(string, string) (io.ReadCloser, *storage.ObjectMeta, error)
	HeadObject(string, string) (*storage.ObjectMeta, error)
	DeleteObject(string, string) error
	ListObjects(string, string, int, string) ([]storage.ObjectMeta, bool, error)
	ListObjectPage(string, string, string, string, int) (storage.ObjectPage, error)
	StatsForPolka(string) (storage.PolkaStats, error)
	DiskUsage() (uint64, uint64, error)
	QuotaUsage() (int64, int64, error)
	UploadDir() string
	CheckFreeSpace(uint64) error
	CreateMultipartUpload(string, string, string) (string, error)
	UploadPart(string, int, io.Reader) (*storage.MultipartPart, error)
	ListParts(string) ([]storage.MultipartPart, error)
	AssembleMultipartUpload(string, []storage.MultipartPart) (*storage.ObjectMeta, error)
	AbortMultipartUpload(string) error
	PinMultipart(string) (func(), error)
	ValidateMultipartTarget(string, string, string) error
}
