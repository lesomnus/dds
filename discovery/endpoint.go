package discovery

import (
	"time"

	"github.com/lesomnus/dds/rtps"
)

// Endpoint is a remote writer or reader as last announced over SEDP.
type Endpoint struct {
	rtps.EndpointData
	FirstSeen time.Time
	LastSeen  time.Time
}

// EndpointLostReason says how an endpoint went away.
type EndpointLostReason int

const (
	// EndpointDisposed: its participant announced it gone.
	EndpointDisposed EndpointLostReason = iota + 1
	// EndpointParticipantLost: its participant went away.
	EndpointParticipantLost
)

func (r EndpointLostReason) String() string {
	switch r {
	case EndpointDisposed:
		return "disposed"
	case EndpointParticipantLost:
		return "participant_lost"
	}
	return "unknown"
}

type (
	// EndpointDiscovered: announced for the first time.
	EndpointDiscovered struct{ Endpoint Endpoint }
	// EndpointUpdated: announced again with something different (its QoS,
	// say).
	EndpointUpdated struct{ Endpoint Endpoint }
	// EndpointLost: gone, and why.
	EndpointLost struct {
		Endpoint Endpoint
		Reason   EndpointLostReason
		At       time.Time
	}
)

func (EndpointDiscovered) isEvent() {}
func (EndpointUpdated) isEvent()    {}
func (EndpointLost) isEvent()       {}

// SameEndpoint compares what matters of two announcements of an endpoint.
func SameEndpoint(a, b *rtps.EndpointData) bool {
	if a.Kind != b.Kind || a.GUID != b.GUID || a.Topic != b.Topic || a.Type != b.Type ||
		a.HasTypeInformation != b.HasTypeInformation || !sameLocators(a.Unicast, b.Unicast) ||
		!sameLocators(a.Multicast, b.Multicast) {
		return false
	}
	qa, qb := a.QoS, b.QoS
	if qa.Reliability != qb.Reliability || qa.MaxBlockingTime != qb.MaxBlockingTime || qa.Durability != qb.Durability ||
		qa.History != qb.History || qa.Depth != qb.Depth || qa.Deadline != qb.Deadline ||
		qa.LatencyBudget != qb.LatencyBudget || qa.Liveliness != qb.Liveliness || qa.LeaseDuration != qb.LeaseDuration ||
		qa.Lifespan != qb.Lifespan || qa.Ownership != qb.Ownership || qa.Strength != qb.Strength ||
		qa.DestinationOrder != qb.DestinationOrder || string(qa.UserData) != string(qb.UserData) ||
		string(qa.TopicData) != string(qb.TopicData) || string(qa.GroupData) != string(qb.GroupData) ||
		len(qa.Partition) != len(qb.Partition) {
		return false
	}
	for i := range qa.Partition {
		if qa.Partition[i] != qb.Partition[i] {
			return false
		}
	}
	return true
}
