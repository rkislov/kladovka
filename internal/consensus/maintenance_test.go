// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"testing"
	"time"
)

func TestMaintenanceDrainsLeaderAndSurvivesRestart(t *testing.T) {
	f := newFixture(t)
	li := f.leader(t)
	target := f.nodes[li]
	node := target.cfg.NodeID
	if !target.BeginRequest() {
		t.Fatal("request rejected before maintenance")
	}
	if e := target.Maintain("service-one", node, "enter"); e != nil {
		t.Fatal(e)
	}
	if target.BeginRequest() {
		target.EndRequest()
		t.Fatal("admitted a request while draining")
	}
	if e := target.maintenanceStep(); e == nil {
		t.Fatal("leader transferred with request in flight")
	}
	if !target.IsLeader() {
		t.Fatal("in-flight request interrupted by handoff")
	}
	target.EndRequest()
	eventually(t, 15*time.Second, func() bool {
		for _, s := range f.nodes {
			m := s.Maintenance()
			if s.IsLeader() && s != target && m != nil && m.Ready {
				return true
			}
		}
		return false
	})
	leader := f.nodes[f.leader(t)]
	if e := leader.Maintain("service-one", node, "check"); e != nil {
		t.Fatal(e)
	}
	if leader.voterCount() != 3 || leader.TargetRF() != 3 {
		t.Fatal("maintenance changed voting configuration")
	}
	if e := leader.Maintain("service-two", leader.cfg.NodeID, "enter"); e == nil {
		t.Fatal("parallel maintenance accepted")
	}
	if e := leader.ChangeMembership("forbidden", "remove", "", leader.cfg.NodeID, ""); e == nil {
		t.Fatal("membership accepted during maintenance")
	}
	if e := target.raft.Snapshot().Error(); e != nil {
		t.Fatal("snapshot maintenance", e)
	}
	target.Close()
	if e := leader.CreatePolka("during-service"); e != nil {
		t.Fatal(e)
	}
	if e := leader.Maintain("service-one", node, "exit"); e == nil {
		t.Fatal("offline member returned to service")
	}
	f.restart(t, li)
	target = f.nodes[li]
	if target.BeginRequest() {
		target.EndRequest()
		t.Fatal("admitted immediately after restart")
	}
	eventually(t, 10*time.Second, func() bool { return target.LocalMaintenance() })
	if target.BeginRequest() {
		target.EndRequest()
		t.Fatal("restart forgot maintenance")
	}
	eventually(t, 10*time.Second, func() bool { return leader.Maintain("service-one", node, "exit") == nil })
	eventually(t, 10*time.Second, func() bool { return !target.LocalMaintenance() })
	if !target.BeginRequest() {
		t.Fatal("requests still blocked after exit")
	}
	target.EndRequest()
	if e := leader.Maintain("service-one", node, "exit"); e != nil {
		t.Fatal("exit retry", e)
	}
	if e := leader.Maintain("service-one", node, "enter"); e != nil {
		t.Fatal("enter retry", e)
	}
	if leader.maintenanceActive() {
		t.Fatal("completed ID started new maintenance")
	}
}
func TestMaintenanceRejectsLossOfRemainingQuorum(t *testing.T) {
	f := newFixture(t)
	leader := f.nodes[f.leader(t)]
	targetIndex := -1
	otherIndex := -1
	for i, s := range f.nodes {
		if s == leader {
			continue
		}
		if targetIndex < 0 {
			targetIndex = i
		} else {
			otherIndex = i
		}
	}
	target := f.nodes[targetIndex]
	if e := leader.Maintain("service", target.cfg.NodeID, "enter"); e != nil {
		t.Fatal(e)
	}
	eventually(t, 15*time.Second, func() bool { m := leader.Maintenance(); return m != nil && m.Ready })
	f.nodes[otherIndex].Close()
	// Leader and serviced voter still form the old quorum. Stopping the serviced
	// voter now would lose quorum and must fail the immediate pre-stop check.
	if e := leader.Maintain("service", target.cfg.NodeID, "check"); e == nil {
		t.Fatal("unsafe stop check succeeded")
	}
	if e := leader.CreatePolka("still-two-votes"); e != nil {
		t.Fatal("serviced member should retain its vote", e)
	}
}
