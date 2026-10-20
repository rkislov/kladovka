// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// Replacement is durable intent. Each configuration change is committed separately.
type Replacement struct {
	NodeID     string `json:"node_id"`
	OldID      string `json:"old_id"`
	NewID      string `json:"new_id"`
	Address    string `json:"address"`
	OldAddress string `json:"old_address"`
	Done       bool   `json:"done"`
}
type Member struct {
	NodeID   string `json:"node_id"`
	RaftID   string `json:"raft_id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}
type candidate struct {
	Leader         bool   `json:"leader"`
	MaintenanceID  string `json:"maintenance_id,omitempty"`
	ActiveRequests int64  `json:"active_requests"`
	ClusterID      string `json:"cluster_id"`
	NodeID         string `json:"node_id"`
	RaftID         string `json:"raft_id"`
	Address        string `json:"address"`
	ReadyIndex     uint64 `json:"ready_index"`
	HTTPURL        string `json:"http_url"`
	Protocol       int    `json:"membership_protocol"`
}

func slotForID(id string) string { slot, _, _ := strings.Cut(id, "#"); return slot }
func validIncarnation(slot, id string) bool {
	prefix := slot + "#"
	if !strings.HasPrefix(id, prefix) {
		return false
	}
	b, e := hex.DecodeString(strings.TrimPrefix(id, prefix))
	return e == nil && len(b) == 16 && id == prefix+hex.EncodeToString(b)
}
func (s *Store) members() []Member {
	f := s.raft.GetConfiguration()
	if f.Error() != nil {
		return nil
	}
	out := []Member{}
	for _, m := range f.Configuration().Servers {
		out = append(out, Member{slotForID(string(m.ID)), string(m.ID), string(m.Address), m.Suffrage.String()})
	}
	return out
}
func (s *Store) voterCount() int {
	n := 0
	for _, m := range s.members() {
		if m.Suffrage == raft.Voter.String() {
			n++
		}
	}
	return n
}
func (s *Store) requiredQuorum() int {
	n := s.voterCount()
	if n == 0 {
		return 0
	}
	return n/2 + 1
}
func (s *Store) replacement() *Replacement {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	if s.fsm.state.Replacement == nil {
		return nil
	}
	r := *s.fsm.state.Replacement
	return &r
}

// CandidateHandler never grants votes. The quorum leader alone admits this incarnation.
func (s *Store) CandidateHandler(w http.ResponseWriter, r *http.Request) {
	if !validIncarnation(s.cfg.NodeID, string(s.localID)) || s.cfg.RaftReplacementAddr == "" {
		http.Error(w, "not a joining candidate", 409)
		return
	}
	s.ReadyHandler(w, r)
}

// ReadyHandler verifies a pinned committed generation for membership operations.
func (s *Store) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}
	address := s.cfg.RaftReplacementAddr
	if address == "" {
		address = s.cfg.RaftPeers[s.cfg.NodeID]
	}
	maintenanceID, active := s.requestState()
	info := candidate{ClusterID: s.cfg.RaftClusterID, NodeID: s.cfg.NodeID, RaftID: string(s.localID), Address: address, HTTPURL: strings.TrimRight(s.cfg.NodeURL, "/"), Protocol: 3, ActiveRequests: active, MaintenanceID: maintenanceID}
	if s.IsLeader() {
		if s.Barrier() != nil {
			http.Error(w, "leader unavailable", 503)
			return
		}
		info.Leader = true
	}
	s.fsm.mu.RLock()
	info.ReadyIndex = s.fsm.state.Index
	s.fsm.mu.RUnlock()
	if r.URL.Query().Get("verify") == "true" {
		index, e := s.verifiedIndex(r.Context())
		if e != nil {
			http.Error(w, "committed data not ready", 503)
			return
		}
		info.ReadyIndex = index
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(info)
}
func (s *Store) verifiedIndex(ctx context.Context) (uint64, error) {
	snap, e := s.fsm.Snapshot()
	if e != nil {
		return 0, e
	}
	defer snap.Release()
	st := snap.(*snapshot).s
	for hash := range st.Chunks {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if _, e := s.fsm.chunk(hash); e != nil {
			return 0, e
		}
	}
	return st.Index, nil
}
func (s *Store) candidateInfo(slot string, verify bool) (candidate, error) {
	return s.peerInfo(s.nodeURL(slot), slot, "candidate", verify)
}
func (s *Store) peerInfo(base, slot, path string, verify bool) (candidate, error) {
	var info candidate
	if base == "" {
		return info, errors.New("unknown member endpoint")
	}
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, "GET", base+"/internal/consensus/"+path+"?verify="+fmt.Sprint(verify), nil)
	if e != nil {
		return info, e
	}
	s.reg.Authorize(req)
	client := *s.reg.Client()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return errors.New("membership endpoint redirects are forbidden")
	}
	resp, e := client.Do(req)
	if e != nil {
		return info, e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return info, errors.New("member verification unavailable")
	}
	e = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&info)
	if e == nil && (info.ClusterID != s.cfg.RaftClusterID || info.NodeID != slot || (path == "candidate" && !validIncarnation(slot, info.RaftID))) {
		e = errors.New("member identity differs")
	}
	return info, e
}

// Replace admits only an explicitly selected fresh incarnation at a previously unused port.
func addressPort(address string) int {
	_, p, e := net.SplitHostPort(address)
	if e != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
}
func (s *Store) Replace(slot, newID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if e := s.Barrier(); e != nil {
		return e
	}
	if slot == s.cfg.NodeID || s.nodeURL(slot) == "" || !validIncarnation(slot, newID) {
		return errors.New("replace a configured non-leader slot with a fresh incarnation")
	}
	if s.maintenanceActive() {
		return errors.New("maintenance active")
	}
	if op := s.membershipChange(); op != nil && !op.Done {
		return errors.New("membership operation pending")
	}
	if r := s.replacement(); r != nil && r.NewID == newID {
		return nil
	} else if r != nil && !r.Done {
		return errors.New("another replacement is pending")
	}
	info, e := s.candidateInfo(slot, false)
	if e != nil {
		return e
	}
	if info.RaftID != newID {
		return errors.New("candidate incarnation differs")
	}
	if addressPort(info.Address) < 1 || addressPort(info.Address) > 65535 {
		return errors.New("invalid candidate raft port")
	}
	for _, address := range s.cfg.RaftPeers {
		if addressPort(info.Address) == addressPort(address) {
			return errors.New("original raft address cannot be reused")
		}
	}
	s.fsm.mu.RLock()
	for _, address := range s.fsm.state.RetiredAddresses {
		if addressPort(info.Address) == addressPort(address) {
			s.fsm.mu.RUnlock()
			return errors.New("retired raft address cannot be reused")
		}
	}
	s.fsm.mu.RUnlock()
	var old Member
	for _, m := range s.members() {
		if m.RaftID == newID || addressPort(m.Address) == addressPort(info.Address) {
			return errors.New("candidate is already a member")
		}
		if m.NodeID == slot {
			old = m
		}
	}
	if old.RaftID == "" || old.Suffrage != raft.Voter.String() {
		return errors.New("slot has no voting member")
	}
	return s.apply(command{Kind: "replace", Replacement: &Replacement{NodeID: slot, OldID: old.RaftID, NewID: newID, Address: info.Address, OldAddress: old.Address}})
}

// One bounded, restartable step; writeMu excludes other catalogue mutations until
// a verified candidate has been promoted or this iteration yields.
func (s *Store) reconcileReplacement() {
	if !s.IsLeader() || !s.writeMu.TryLock() {
		return
	}
	defer s.writeMu.Unlock()
	e := s.replacementStep()
	s.repairMu.Lock()
	s.replacementError = ""
	if e != nil {
		s.replacementError = e.Error()
	}
	s.repairMu.Unlock()
}
func (s *Store) replacementStep() error {
	r := s.replacement()
	if r == nil || r.Done {
		return nil
	}
	if e := s.Barrier(); e != nil {
		return e
	}
	f := s.raft.GetConfiguration()
	if e := f.Error(); e != nil {
		return e
	}
	for _, m := range f.Configuration().Servers {
		if string(m.ID) == r.OldID {
			return s.raft.RemoveServer(m.ID, f.Index(), 5*time.Second).Error()
		}
	}
	for _, m := range f.Configuration().Servers {
		if string(m.ID) != r.NewID {
			continue
		}
		if string(m.Address) != r.Address {
			return errors.New("replacement address differs from committed intent")
		}
		if m.Suffrage == raft.Voter {
			return s.apply(command{Kind: "replace_done", Replacement: r})
		}
		s.fsm.mu.RLock()
		index := s.fsm.state.Index
		s.fsm.mu.RUnlock()
		info, e := s.candidateInfo(r.NodeID, true)
		if e != nil {
			return e
		}
		if info.RaftID != r.NewID || info.Address != r.Address {
			return errors.New("replacement candidate changed")
		}
		if info.ReadyIndex < index {
			return fmt.Errorf("candidate is catching up: index %d of %d", info.ReadyIndex, index)
		}
		if e := s.Barrier(); e != nil {
			return e
		}
		return s.raft.AddVoter(raft.ServerID(r.NewID), raft.ServerAddress(r.Address), f.Index(), 5*time.Second).Error()
	}
	return s.raft.AddNonvoter(raft.ServerID(r.NewID), raft.ServerAddress(r.Address), f.Index(), 5*time.Second).Error()
}
