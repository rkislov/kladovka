// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"time"
)

// Maintenance reserves one voting member for temporary servicing. Ready records
// a completed drain, not a promise that the remaining quorum stays available.
type Maintenance struct {
	RequestID string `json:"request_id"`
	NodeID    string `json:"node_id"`
	RaftID    string `json:"raft_id"`
	Ready     bool   `json:"ready"`
	Done      bool   `json:"done"`
}

func (s *Store) Maintenance() *Maintenance {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if s.fsm.state.Maintenance == nil {
		return nil
	}
	m := *s.fsm.state.Maintenance
	return &m
}
func (s *Store) maintenanceActive() bool { m := s.Maintenance(); return m != nil && !m.Done }
func (s *Store) LocalMaintenance() bool {
	m := s.Maintenance()
	return m != nil && !m.Done && m.RaftID == string(s.localID)
}

// BeginRequest pairs with EndRequest around the complete HTTP stream. Holding
// the FSM read lock makes admission atomic with the replicated drain command.
func (s *Store) BeginRequest() bool {
	// Raft replays asynchronously after Open. Before the first admission, catch
	// up to a leader-confirmed generation so restart cannot forget a drain.
	if !s.admissionReady.Load() {
		if s.IsLeader() {
			if s.Barrier() != nil {
				return false
			}
		} else {
			_, leaderID := s.raft.LeaderWithID()
			info, err := s.peerInfo(s.LeaderURL(), slotForID(string(leaderID)), "ready", false)
			if err != nil || !info.Leader || info.RaftID != string(leaderID) {
				return false
			}
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				s.fsm.mu.RLock()
				caughtUp := s.fsm.state.Index >= info.ReadyIndex
				s.fsm.mu.RUnlock()
				if caughtUp {
					break
				}
				select {
				case <-s.ctx.Done():
					return false
				case <-deadline.C:
					return false
				case <-tick.C:
				}
			}
		}
		s.admissionReady.Store(true)
	}
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	m := s.fsm.state.Maintenance
	if m != nil && !m.Done && m.RaftID == string(s.localID) {
		return false
	}
	s.activeRequests.Add(1)
	return true
}
func (s *Store) EndRequest() { s.activeRequests.Add(-1) }
func (s *Store) Maintain(id, node, action string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if e := s.Barrier(); e != nil {
		return e
	}
	if !requestIDPattern.MatchString(id) || node == "" {
		return errors.New("request_id and node_id required")
	}
	m := s.Maintenance()
	if action == "enter" {
		s.fsm.mu.RLock()
		old, exists := s.fsm.state.CompletedMaintenance[id]
		s.fsm.mu.RUnlock()
		if exists {
			if old.NodeID == node {
				return nil
			}
			return errors.New("request_id already used")
		}
		if m != nil && !m.Done {
			if m.RequestID == id && m.NodeID == node {
				return nil
			}
			return errors.New("another maintenance operation is active")
		}
		if c := s.membershipChange(); c != nil && !c.Done {
			return errors.New("membership operation pending")
		}
		if r := s.replacement(); r != nil && !r.Done {
			return errors.New("replacement pending")
		}
		members := s.members()
		var target Member
		for _, v := range members {
			if v.Suffrage != raft.Voter.String() {
				return errors.New("learner admission pending")
			}
			info, e := s.peerInfo(s.nodeURL(v.NodeID), v.NodeID, "ready", false)
			if e != nil {
				return e
			}
			if info.Protocol < 3 || info.RaftID != v.RaftID {
				return errors.New("upgrade every voter to maintenance protocol 3 first")
			}
			if v.NodeID == node {
				target = v
			}
		}
		if target.RaftID == "" {
			return errors.New("unknown voting member")
		}
		m = &Maintenance{RequestID: id, NodeID: node, RaftID: target.RaftID}
		if _, e := s.maintenanceQuorum(m); e != nil {
			return e
		}
		return s.apply(command{Kind: "maintenance_enter", Maintenance: m})
	}
	if action != "check" && action != "exit" {
		return errors.New("action must be enter, check or exit")
	}
	if m == nil || m.RequestID != id || m.NodeID != node {
		return errors.New("maintenance intent differs")
	}
	if m.Done {
		if action == "exit" {
			return nil
		}
		return errors.New("maintenance already completed")
	}
	if action == "check" {
		if !m.Ready {
			return errors.New("maintenance drain pending")
		}
		if m.RaftID == string(s.localID) {
			return errors.New("maintenance member is leader; wait for handoff")
		}
		_, e := s.maintenanceQuorum(m)
		return e
	}
	// The original member must return with its persisted identity and all committed
	// chunks before serving requests again. No cancellation may forget its vote.
	s.fsm.mu.RLock()
	index := s.fsm.state.Index
	s.fsm.mu.RUnlock()
	info, e := s.peerInfo(s.nodeURL(node), node, "ready", true)
	if e != nil {
		return e
	}
	if info.RaftID != m.RaftID || info.ReadyIndex < index || info.MaintenanceID != id {
		return errors.New("returning member has not verified committed data")
	}
	return s.apply(command{Kind: "maintenance_exit", Maintenance: m})
}

// A majority of the unchanged voting configuration must remain, not a majority
// of a hypothetically smaller group. The serviced member keeps its vote.
func (s *Store) maintenanceQuorum(m *Maintenance) ([]Member, error) {
	s.fsm.mu.RLock()
	index := s.fsm.state.Index
	s.fsm.mu.RUnlock()
	members := s.members()
	ready := []Member{}
	voters := 0
	for _, v := range members {
		if v.Suffrage != raft.Voter.String() {
			continue
		}
		voters++
		if v.RaftID == m.RaftID {
			continue
		}
		info, e := s.peerInfo(s.nodeURL(v.NodeID), v.NodeID, "ready", true)
		if e == nil && info.Protocol >= 3 && info.RaftID == v.RaftID && info.Address == v.Address && info.ReadyIndex >= index && info.MaintenanceID == "" {
			ready = append(ready, v)
		}
	}
	if voters < 3 || len(ready) < voters/2+1 {
		return nil, fmt.Errorf("verified remaining quorum unavailable: %d ready of %d voters", len(ready), voters)
	}
	if e := s.Barrier(); e != nil {
		return nil, e
	}
	return ready, nil
}
func (s *Store) reconcileMaintenance() {
	if !s.IsLeader() || !s.writeMu.TryLock() {
		return
	}
	defer s.writeMu.Unlock()
	e := s.maintenanceStep()
	s.repairMu.Lock()
	defer s.repairMu.Unlock()
	s.maintenanceError = ""
	if e != nil {
		s.maintenanceError = e.Error()
	}
}
func (s *Store) maintenanceStep() error {
	m := s.Maintenance()
	if m == nil || m.Done {
		return nil
	}
	if e := s.Barrier(); e != nil {
		return e
	}
	if m.Ready && m.RaftID != string(s.localID) {
		return nil
	}
	if m.RaftID == string(s.localID) && s.activeRequests.Load() != 0 {
		return errors.New("waiting for leader requests to drain")
	}
	remaining, e := s.maintenanceQuorum(m)
	if e != nil {
		return e
	}
	if m.RaftID == string(s.localID) {
		return s.raft.LeadershipTransferToServer(raft.ServerID(remaining[0].RaftID), raft.ServerAddress(remaining[0].Address)).Error()
	}
	if m.Ready {
		return nil
	}
	info, e := s.peerInfo(s.nodeURL(m.NodeID), m.NodeID, "ready", false)
	if e != nil {
		return e
	}
	if info.RaftID != m.RaftID || info.MaintenanceID != m.RequestID || info.ActiveRequests != 0 {
		return errors.New("waiting for member to drain active requests")
	}
	return s.apply(command{Kind: "maintenance_ready", Maintenance: m})
}

// requestState pairs the local drain marker with its in-flight count under the
// same FSM lock used for request admission. A pre-drain count must never be
// returned alongside a post-drain marker.
func (s *Store) requestState() (string, int64) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	m := s.fsm.state.Maintenance
	id := ""
	if m != nil && !m.Done && m.RaftID == string(s.localID) {
		id = m.RequestID
	}
	return id, s.activeRequests.Load()
}
