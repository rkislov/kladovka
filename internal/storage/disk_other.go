//go:build !linux && !darwin

// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package storage

func diskUsage(root string) (uint64, uint64, error) { return 0, 0, nil }
