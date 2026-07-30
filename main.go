package main

import (
	"fmt"
	"sync"
)

type Partition string
type MemberID string

type CooperativeStickyAssignor struct {
	mu               sync.Mutex
	activeMembers    map[MemberID]bool
	ownedPartitions  map[MemberID][]Partition
	assignedPartitions map[Partition]MemberID
}

func NewCooperativeStickyAssignor() *CooperativeStickyAssignor {
	return &CooperativeStickyAssignor{
		activeMembers:    make(map[MemberID]bool),
		ownedPartitions:  make(map[MemberID][]Partition),
		assignedPartitions: make(map[Partition]MemberID),
	}
}

func (c *CooperativeStickyAssignor) RegisterMember(member MemberID, partitions []Partition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeMembers[member] = true
	c.ownedPartitions[member] = partitions
	for _, p := range partitions {
		c.assignedPartitions[p] = member
	}
}

func (c *CooperativeStickyAssignor) HandleSessionTimeout(member MemberID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	fmt.Printf("Member %s timed out. Clearing its partition ownership to prevent orphanage.\n", member)
	delete(c.activeMembers, member)
	
	// Explicitly clear ownership of partitions previously held by the timed-out member
	for _, p := range c.ownedPartitions[member] {
		delete(c.assignedPartitions, p)
	}
	delete(c.ownedPartitions, member)
}

func (c *CooperativeStickyAssignor) Rebalance(allPartitions []Partition) map[MemberID][]Partition {
	c.mu.Lock()
	defer c.mu.Unlock()

	newAssignment := make(map[MemberID][]Partition)
	if len(c.activeMembers) == 0 {
		return newAssignment
	}

	// Reconcile active members against the union of all claimed partitions
	activeMembersList := make([]MemberID, 0, len(c.activeMembers))
	for m := range c.activeMembers {
		activeMembersList = append(activeMembersList, m)
		newAssignment[m] = []Partition{}
	}

	// Assign partitions to active members
	for i, p := range allPartitions {
		// If the partition was owned by a timed-out member, it is now free to be assigned immediately
		memberIdx := i % len(activeMembersList)
		targetMember := activeMembersList[memberIdx]
		newAssignment[targetMember] = append(newAssignment[targetMember], p)
		c.assignedPartitions[p] = targetMember
	}

	return newAssignment
}

func main() {
	fmt.Println("Simulating CooperativeStickyAssignor Partition Orphanage Fix...")
	
	assignor := NewCooperativeStickyAssignor()
	assignor.RegisterMember("consumer-1", []Partition{"p0", "p1"})
	assignor.RegisterMember("consumer-2", []Partition{"p2", "p3"})
	assignor.RegisterMember("consumer-3", []Partition{"p4", "p5"})

	// Simulate consumer-3 heartbeat expiration mid-rebalance
	assignor.HandleSessionTimeout("consumer-3")

	// Trigger corrective rebalance
	allPartitions := []Partition{"p0", "p1", "p2", "p3", "p4", "p5"}
	assignments := assignor.Rebalance(allPartitions)

	fmt.Println("New Assignments after corrective rebalance:")
	for member, partitions := range assignments {
		fmt.Printf("  %s: %v\n", member, partitions)
	}
}
