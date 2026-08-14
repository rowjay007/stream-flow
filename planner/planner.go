package planner

import "fmt"

// Simple distributed planner that assigns fragments to processor addresses.
type Planner struct{}

type PlanFragment struct {
	ID      string
	Address string
	// For simplicity, we store the list of fields to project/aggregate.
	Projections []string
}

// NewPlanner creates a Planner.
func NewPlanner() *Planner { return &Planner{} }

// Distribute returns a fragment per address using round-robin assignment.
func (p *Planner) Distribute(addresses []string, projections []string) []PlanFragment {
	var frags []PlanFragment
	for i, addr := range addresses {
		frags = append(frags, PlanFragment{ID: fmt.Sprintf("%d", i), Address: addr, Projections: projections})
	}
	return frags
}
