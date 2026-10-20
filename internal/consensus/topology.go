// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rkislov/kladovka/internal/cluster"
)

type Endpoint struct {
	NodeID  string `json:"node_id"`
	HTTPURL string `json:"http_url"`
	RaftID  string `json:"raft_id"`
	Address string `json:"address"`
	Removed bool   `json:"removed,omitempty"`
}
type MembershipChange struct {
	ID       string   `json:"request_id"`
	Kind     string   `json:"kind"`
	Node     Endpoint `json:"node"`
	TargetRF int      `json:"target_rf"`
	Done     bool     `json:"done"`
}

func (s *Store) membershipChange() *MembershipChange {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if s.fsm.state.Change == nil {
		return nil
	}
	c := *s.fsm.state.Change
	return &c
}
func (s *Store) nodeURL(id string) string {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if s.fsm.state.Topology != nil {
		n := s.fsm.state.Topology[id]
		if n.Removed {
			return ""
		}
		return n.HTTPURL
	}
	return s.reg.GetURL(id)
}
func (s *Store) PeerNodes() []cluster.NodeState {
	s.SyncTopology()
	return s.reg.Snapshot()
}
func (s *Store) SyncTopology() {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if s.fsm.state.Topology == nil {
		return
	}
	urls := map[string]string{}
	for id, n := range s.fsm.state.Topology {
		if !n.Removed {
			urls[id] = n.HTTPURL
		}
	}
	s.reg.SyncTopology(urls)
}
func (s *Store) TargetRF() int {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if s.fsm.state.Change != nil {
		return s.fsm.state.Change.TargetRF
	}
	return s.cfg.ReplicationFactor
}
func (s *Store) CanRoute() bool {
	for _, m := range s.members() {
		if m.RaftID == string(s.localID) && m.Suffrage == raft.Voter.String() {
			return true
		}
	}
	return false
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ChangeMembership commits intent before touching the Raft configuration. Reusing
// a request ID is safe only with the identical operation and destination.
func (s *Store) ChangeMembership(id, kind, httpURL, nodeID, raftID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if e := s.Barrier(); e != nil {
		return e
	}
	if !requestIDPattern.MatchString(id) || (kind != "add" && kind != "remove") {
		return errors.New("invalid membership request")
	}
	httpURL = strings.TrimRight(httpURL, "/")
	if kind == "add" {
		u, e := url.Parse(httpURL)
		if e != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
			return errors.New("invalid member HTTP URL")
		}
		if u.Scheme == "http" {
			ip := net.ParseIP(u.Hostname())
			if ip == nil || !ip.IsLoopback() {
				return errors.New("remote member requires HTTPS")
			}
		}
		nodeID = u.Host
		if !validIncarnation(nodeID, raftID) {
			return errors.New("fresh candidate raft_id required")
		}
	}
	s.fsm.mu.RLock()
	old, exists := s.fsm.state.CompletedChanges[id]
	if !exists && s.fsm.state.Change != nil && s.fsm.state.Change.ID == id {
		old = *s.fsm.state.Change
		exists = true
	}
	topology := map[string]Endpoint{}
	for k, v := range s.fsm.state.Topology {
		topology[k] = v
	}
	retired := append([]string(nil), s.fsm.state.RetiredAddresses...)
	s.fsm.mu.RUnlock()
	if exists {
		if old.Kind == kind && old.Node.NodeID == nodeID && (kind == "remove" || (old.Node.RaftID == raftID && old.Node.HTTPURL == httpURL)) {
			return nil
		}
		return errors.New("request_id already used for another operation")
	}
	if c := s.membershipChange(); c != nil && !c.Done {
		return errors.New("membership operation pending")
	}
	if c := s.replacement(); c != nil && !c.Done {
		return errors.New("replacement pending")
	}
	members := s.members()
	count := 0
	for _, m := range members {
		if m.Suffrage != raft.Voter.String() {
			return errors.New("configuration contains a learner")
		}
		count++
		if _, ok := topology[m.NodeID]; !ok {
			topology[m.NodeID] = Endpoint{NodeID: m.NodeID, HTTPURL: s.nodeURL(m.NodeID), RaftID: m.RaftID, Address: m.Address}
		}
	}
	change := MembershipChange{ID: id, Kind: kind, TargetRF: count}
	if kind == "add" {
		if count >= 5 {
			return errors.New("maximum five voters")
		}
		if _, ok := topology[nodeID]; ok {
			return errors.New("member slot already used; use replacement for an active slot")
		}
		info, e := s.peerInfo(httpURL, nodeID, "candidate", false)
		if e != nil {
			return e
		}
		if info.Protocol < 2 || info.RaftID != raftID || info.HTTPURL != httpURL {
			return errors.New("candidate identity or protocol differs")
		}
		port := addressPort(info.Address)
		if port < 1 || port > 65535 {
			return errors.New("invalid candidate raft port")
		}
		for _, a := range s.cfg.RaftPeers {
			retired = append(retired, a)
		}
		for _, n := range topology {
			retired = append(retired, n.Address)
		}
		for _, a := range retired {
			if port == addressPort(a) {
				return errors.New("raft port has already been used")
			}
		}
		change.Node = Endpoint{NodeID: nodeID, HTTPURL: httpURL, RaftID: raftID, Address: info.Address}
		change.TargetRF++
	} else {
		n, ok := topology[nodeID]
		if !ok || n.Removed {
			return errors.New("unknown active member")
		}
		if count <= 3 {
			return errors.New("minimum three voters")
		}
		change.Node = n
		change.TargetRF--
	}
	// Every remaining member must understand the new durable topology commands.
	for _, m := range members {
		if kind == "remove" && m.NodeID == nodeID {
			continue
		}
		if m.RaftID == string(s.localID) {
			continue
		}
		info, e := s.peerInfo(topology[m.NodeID].HTTPURL, m.NodeID, "ready", false)
		if e != nil {
			return e
		}
		if info.Protocol < 2 || info.RaftID != m.RaftID {
			return errors.New("upgrade all remaining members before changing membership")
		}
	}
	return s.apply(command{Kind: "membership_change", Change: &change, Topology: topology})
}
func (s *Store) reconcileMembership() {
	if !s.IsLeader() || !s.writeMu.TryLock() {
		return
	}
	defer s.writeMu.Unlock()
	e := s.membershipStep()
	s.repairMu.Lock()
	defer s.repairMu.Unlock()
	s.membershipError = ""
	if e != nil {
		s.membershipError = e.Error()
	}
}
func (s *Store) membershipStep() error {
	c := s.membershipChange()
	if c == nil || c.Done {
		return nil
	}
	if e := s.Barrier(); e != nil {
		return e
	}
	f := s.raft.GetConfiguration()
	if e := f.Error(); e != nil {
		return e
	}
	var target *raft.Server
	for _, m := range f.Configuration().Servers {
		if string(m.ID) == c.Node.RaftID {
			v := m
			target = &v
		}
	}
	if c.Kind == "add" {
		if target == nil {
			return s.raft.AddNonvoter(raft.ServerID(c.Node.RaftID), raft.ServerAddress(c.Node.Address), f.Index(), 5*time.Second).Error()
		}
		if string(target.Address) != c.Node.Address {
			return errors.New("member address differs from intent")
		}
		if target.Suffrage != raft.Voter {
			s.fsm.mu.RLock()
			index := s.fsm.state.Index
			s.fsm.mu.RUnlock()
			info, e := s.peerInfo(c.Node.HTTPURL, c.Node.NodeID, "ready", true)
			if e != nil {
				return e
			}
			if info.RaftID != c.Node.RaftID || info.Address != c.Node.Address || info.ReadyIndex < index {
				return errors.New("learner has not verified committed data")
			}
			if e = s.Barrier(); e != nil {
				return e
			}
			return s.raft.AddVoter(target.ID, target.Address, f.Index(), 5*time.Second).Error()
		}
	} else if target != nil {
		s.fsm.mu.RLock()
		index := s.fsm.state.Index
		s.fsm.mu.RUnlock()
		ready := []raft.Server{}
		for _, m := range f.Configuration().Servers {
			if m.ID == target.ID || m.Suffrage != raft.Voter {
				continue
			}
			info, e := s.peerInfo(s.nodeURL(slotForID(string(m.ID))), slotForID(string(m.ID)), "ready", true)
			if e == nil && info.Protocol >= 2 && info.RaftID == string(m.ID) && info.Address == string(m.Address) && info.ReadyIndex >= index {
				ready = append(ready, m)
			}
		}
		if len(ready) < c.TargetRF/2+1 {
			return fmt.Errorf("verified remaining quorum unavailable: %d ready", len(ready))
		}
		if e := s.Barrier(); e != nil {
			return e
		}
		if target.ID == s.localID {
			return s.raft.LeadershipTransferToServer(ready[0].ID, ready[0].Address).Error()
		}
		return s.raft.RemoveServer(target.ID, f.Index(), 5*time.Second).Error()
	}
	return s.apply(command{Kind: "membership_done", Change: c})
}
