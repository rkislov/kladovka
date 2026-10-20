// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import "strconv"

// Observations are local, read-only diagnostics. They do not run a quorum
// probe or claim that an HTTP-healthy member can acknowledge a Raft write.
func (s *Store) Observations() map[string]uint64 {
	flag := func(b bool) uint64 {
		if b {
			return 1
		}
		return 0
	}
	stats := s.raft.Stats()
	number := func(k string) uint64 { n, _ := strconv.ParseUint(stats[k], 10, 64); return n }
	commit, applied := number("commit_index"), number("applied_index")
	lag := uint64(0)
	if commit > applied {
		lag = commit - applied
	}
	s.repairMu.Lock()
	repairError := s.repairError != ""
	operationError := s.membershipError != "" || s.replacementError != ""
	s.repairMu.Unlock()
	c, r := s.membershipChange(), s.replacement()
	pending := (c != nil && !c.Done) || (r != nil && !r.Done)
	return map[string]uint64{
		"voters": uint64(s.voterCount()), "target_rf": uint64(s.TargetRF()),
		"leader_known": flag(s.LeaderURL() != ""), "term": number("term"),
		"commit_index": commit, "raft_applied_index": applied, "apply_backlog": lag, "fsm_pending": number("fsm_pending"),
		"repair_error": flag(repairError), "membership_pending": flag(pending),
		"membership_error": flag(pending && operationError),
	}
}
