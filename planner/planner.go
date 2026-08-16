package planner

import "fmt"

type Planner struct{}

type PlanFragment struct {
	ID      string
	Address string

	Projections []string
}

func NewPlanner() *Planner { return &Planner{} }

func (p *Planner) Distribute(addresses []string, projections []string) []PlanFragment {
	var frags []PlanFragment
	for i, addr := range addresses {
		frags = append(frags, PlanFragment{ID: fmt.Sprintf("%d", i), Address: addr, Projections: projections})
	}
	return frags
}
