// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");

package cluster

import "testing"

func TestNodesForObjectStable(t *testing.T) {
	ids := []string{"a:8000", "b:8000", "c:8000"}
	a := NodesForObject("полка1", "docs/readme.txt", ids, 2)
	b := NodesForObject("полка1", "docs/readme.txt", ids, 2)
	if len(a) != 2 || a[0] != b[0] || a[1] != b[1] {
		t.Fatalf("unstable placement: %#v vs %#v", a, b)
	}
	if a[0] == a[1] {
		t.Fatal("primary and replica must differ when rf=2 and 3 nodes")
	}
}
