package raft

import "testing"

func TestISRManagerMinISR(t *testing.T) {
	m := NewISRManager(2)
	m.SetLeader(1)
	if m.CanAcceptWrites() {
		t.Fatalf("expected writes to be blocked with only leader in ISR")
	}
	m.MarkInSync(2)
	if !m.CanAcceptWrites() {
		t.Fatalf("expected writes allowed once minISR is reached")
	}
	m.MarkOutOfSync(2)
	if m.CanAcceptWrites() {
		t.Fatalf("expected writes blocked when ISR drops below minISR")
	}
}
