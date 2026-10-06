// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package cluster

import (
	"crypto/sha256"
	"encoding/binary"
)

const defaultSeed = "kladovka-cluster-v1"

// HashKeyToIndex maps a key to [0, cardinality).
func HashKeyToIndex(key string, cardinality int) int {
	if cardinality <= 0 {
		return -1
	}
	h := sha256.Sum256(append([]byte(defaultSeed), []byte(key)...))
	val := binary.BigEndian.Uint64(h[:8])
	return int(val % uint64(cardinality))
}

// NodesForObject returns primary first, then replicas on the ring.
func NodesForObject(polka, objectKey string, nodeIDs []string, replicationFactor int) []string {
	if len(nodeIDs) == 0 {
		return nil
	}
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	if replicationFactor > len(nodeIDs) {
		replicationFactor = len(nodeIDs)
	}
	full := polka + "/" + objectKey
	idx := HashKeyToIndex(full, len(nodeIDs))
	out := make([]string, 0, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		out = append(out, nodeIDs[(idx+i)%len(nodeIDs)])
	}
	return out
}
