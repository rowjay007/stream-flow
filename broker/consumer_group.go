package broker

import (
	"sort"
	"sync"
)

type ConsumerGroupCoordinator struct {
	mu          sync.RWMutex
	members     map[string]map[string]struct{}
	assignments map[string]map[string][]int
}

func NewConsumerGroupCoordinator() *ConsumerGroupCoordinator {
	return &ConsumerGroupCoordinator{
		members:     make(map[string]map[string]struct{}),
		assignments: make(map[string]map[string][]int),
	}
}

func (c *ConsumerGroupCoordinator) Join(group, member string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[group] == nil {
		c.members[group] = make(map[string]struct{})
	}
	c.members[group][member] = struct{}{}
}

func (c *ConsumerGroupCoordinator) Leave(group, member string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[group] == nil {
		return
	}
	delete(c.members[group], member)
	if c.assignments[group] != nil {
		delete(c.assignments[group], member)
	}
}

func (c *ConsumerGroupCoordinator) Rebalance(group string, partitions int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	members := make([]string, 0, len(c.members[group]))
	for m := range c.members[group] {
		members = append(members, m)
	}
	sort.Strings(members)
	if c.assignments[group] == nil {
		c.assignments[group] = make(map[string][]int)
	}
	for _, m := range members {
		c.assignments[group][m] = nil
	}
	if len(members) == 0 {
		return
	}
	for p := 0; p < partitions; p++ {
		m := members[p%len(members)]
		c.assignments[group][m] = append(c.assignments[group][m], p)
	}
}

func (c *ConsumerGroupCoordinator) Assignment(group string) map[string][]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string][]int)
	for member, parts := range c.assignments[group] {
		out[member] = append([]int(nil), parts...)
	}
	return out
}

func (b *Broker) JoinGroup(group, member string) {
	b.coordinator.Join(group, member)
}

func (b *Broker) LeaveGroup(group, member string) {
	b.coordinator.Leave(group, member)
}

func (b *Broker) RebalanceGroup(group string, partitions int) {
	b.coordinator.Rebalance(group, partitions)
}

func (b *Broker) GroupAssignment(group string) map[string][]int {
	return b.coordinator.Assignment(group)
}
