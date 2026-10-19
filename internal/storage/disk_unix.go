//go:build linux || darwin

// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package storage

import "syscall"

// diskUsage reports filesystem capacity, rather than a synthetic percentage.
func diskUsage(root string) (uint64, uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return 0, 0, err
	}
	total := st.Blocks * uint64(st.Bsize)
	used := (st.Blocks - st.Bfree) * uint64(st.Bsize)
	return used, total, nil
}
