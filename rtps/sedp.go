package rtps

import (
	"encoding/binary"
	"time"
)

// Parameter IDs of endpoint discovery data and QoS (RTPS 2.5, 9.6.2.2).
const (
	PIDUnicastLocator     ParameterID = 0x002f
	PIDMulticastLocator   ParameterID = 0x0030
	PIDTopicName          ParameterID = 0x0005
	PIDTypeName           ParameterID = 0x0007
	PIDOwnershipStrength  ParameterID = 0x0006
	PIDReliability        ParameterID = 0x001a
	PIDLiveliness         ParameterID = 0x001b
	PIDDurability         ParameterID = 0x001d
	PIDDurabilityService  ParameterID = 0x001e
	PIDOwnership          ParameterID = 0x001f
	PIDPresentation       ParameterID = 0x0021
	PIDDeadline           ParameterID = 0x0023
	PIDDestinationOrder   ParameterID = 0x0025
	PIDLatencyBudget      ParameterID = 0x0027
	PIDPartition          ParameterID = 0x0029
	PIDLifespan           ParameterID = 0x002b
	PIDGroupData          ParameterID = 0x002d
	PIDTopicData          ParameterID = 0x002e
	PIDHistory            ParameterID = 0x0040
	PIDEndpointGUID       ParameterID = 0x005a
	PIDDataRepresentation ParameterID = 0x0073
	PIDTypeInformation    ParameterID = 0x0075
)

// ReliabilityKind is the RELIABILITY QoS kind as sent on the wire.
type ReliabilityKind uint32

const (
	BestEffort ReliabilityKind = 1
	Reliable   ReliabilityKind = 2
)

func (k ReliabilityKind) String() string {
	switch k {
	case BestEffort:
		return "best_effort"
	case Reliable:
		return "reliable"
	}
	return "unknown"
}

// DurabilityKind is the DURABILITY QoS kind.
type DurabilityKind uint32

const (
	Volatile       DurabilityKind = 0
	TransientLocal DurabilityKind = 1
	Transient      DurabilityKind = 2
	Persistent     DurabilityKind = 3
)

func (k DurabilityKind) String() string {
	return [...]string{"volatile", "transient_local", "transient", "persistent", "unknown"}[min(int(k), 4)]
}

// HistoryKind is the HISTORY QoS kind.
type HistoryKind uint32

const (
	KeepLast HistoryKind = 0
	KeepAll  HistoryKind = 1
)

func (k HistoryKind) String() string {
	return [...]string{"keep_last", "keep_all", "unknown"}[min(int(k), 2)]
}

// LivelinessKind is the LIVELINESS QoS kind.
type LivelinessKind uint32

const (
	LivelinessAutomatic         LivelinessKind = 0
	LivelinessManualParticipant LivelinessKind = 1
	LivelinessManualTopic       LivelinessKind = 2
)

func (k LivelinessKind) String() string {
	return [...]string{"automatic", "manual_by_participant", "manual_by_topic", "unknown"}[min(int(k), 3)]
}

// OwnershipKind is the OWNERSHIP QoS kind.
type OwnershipKind uint32

const (
	Shared    OwnershipKind = 0
	Exclusive OwnershipKind = 1
)

// QoS is what an endpoint announces of its QoS. Absent policies hold the
// defaults the specification gives discovered data.
type QoS struct {
	Reliability     ReliabilityKind
	MaxBlockingTime time.Duration
	Durability      DurabilityKind
	History         HistoryKind
	Depth           int32
	Deadline        time.Duration
	LatencyBudget   time.Duration
	Liveliness      LivelinessKind
	LeaseDuration   time.Duration
	Lifespan        time.Duration
	Ownership       OwnershipKind
	Strength        int32
	// DestinationOrder: 0 by reception, 1 by source timestamp.
	DestinationOrder uint32
	Partition        []string
	UserData         []byte
	TopicData        []byte
	GroupData        []byte
}

// EndpointKind tells writers from readers.
type EndpointKind int

const (
	Writer EndpointKind = iota + 1
	Reader
)

func (k EndpointKind) String() string {
	if k == Writer {
		return "writer"
	}
	return "reader"
}

// EndpointData is what a writer or reader announces over SEDP
// (DiscoveredWriterData / DiscoveredReaderData).
type EndpointData struct {
	Kind        EndpointKind
	GUID        GUID
	Participant GUIDPrefix
	Topic       string
	Type        string
	QoS         QoS
	Unicast     []Locator
	Multicast   []Locator
	// HasTypeInformation: it announced XTypes type information.
	HasTypeInformation bool
}

// defaultQoS is the QoS of discovered data before its parameters (RTPS 2.5,
// 9.6.2.2.5; DDS 1.4, 2.2.3): a reader is best-effort, a writer reliable with
// 100 ms max blocking.
func defaultQoS(kind EndpointKind) QoS {
	q := QoS{
		Reliability:   BestEffort,
		Durability:    Volatile,
		History:       KeepLast,
		Depth:         1,
		Deadline:      DurationInfinite,
		LatencyBudget: 0,
		Liveliness:    LivelinessAutomatic,
		LeaseDuration: DurationInfinite,
		Lifespan:      DurationInfinite,
		Ownership:     Shared,
	}
	if kind == Writer {
		q.Reliability, q.MaxBlockingTime = Reliable, 100*time.Millisecond
	}
	return q
}

// ParseEndpointData reads SEDP endpoint data out of a parameter list. The
// GUID is required; a policy that does not decode keeps its default.
func ParseEndpointData(kind EndpointKind, pl ParameterList) (EndpointData, error) {
	o := pl.Order
	e := EndpointData{Kind: kind, QoS: defaultQoS(kind)}
	gotGUID := false
	for _, p := range pl.Params {
		v := p.Value
		switch p.ID {
		case PIDEndpointGUID:
			g, err := ReadGUID(v)
			if err != nil {
				return e, err
			}
			e.GUID, gotGUID = g, true
		case PIDParticipantGUID:
			if g, err := ReadGUID(v); err == nil {
				e.Participant = g.Prefix
			}
		case PIDTopicName:
			if s, err := ReadString(o, v); err == nil {
				e.Topic = s
			}
		case PIDTypeName:
			if s, err := ReadString(o, v); err == nil {
				e.Type = s
			}
		case PIDUnicastLocator:
			if l, err := ReadLocator(o, v); err == nil {
				e.Unicast = append(e.Unicast, l)
			}
		case PIDMulticastLocator:
			if l, err := ReadLocator(o, v); err == nil {
				e.Multicast = append(e.Multicast, l)
			}
		case PIDReliability:
			if len(v) >= 4 {
				e.QoS.Reliability = ReliabilityKind(o.Uint32(v))
				if d, err := ReadDuration(o, v[4:]); err == nil {
					e.QoS.MaxBlockingTime = d
				}
			}
		case PIDDurability:
			if n, err := ReadUint32(o, v); err == nil {
				e.QoS.Durability = DurabilityKind(n)
			}
		case PIDHistory:
			if len(v) >= 8 {
				e.QoS.History = HistoryKind(o.Uint32(v))
				e.QoS.Depth = int32(o.Uint32(v[4:]))
			}
		case PIDDeadline:
			if d, err := ReadDuration(o, v); err == nil {
				e.QoS.Deadline = d
			}
		case PIDLatencyBudget:
			if d, err := ReadDuration(o, v); err == nil {
				e.QoS.LatencyBudget = d
			}
		case PIDLiveliness:
			if len(v) >= 4 {
				e.QoS.Liveliness = LivelinessKind(o.Uint32(v))
				if d, err := ReadDuration(o, v[4:]); err == nil {
					e.QoS.LeaseDuration = d
				}
			}
		case PIDLifespan:
			if d, err := ReadDuration(o, v); err == nil {
				e.QoS.Lifespan = d
			}
		case PIDOwnership:
			if n, err := ReadUint32(o, v); err == nil {
				e.QoS.Ownership = OwnershipKind(n)
			}
		case PIDOwnershipStrength:
			if n, err := ReadUint32(o, v); err == nil {
				e.QoS.Strength = int32(n)
			}
		case PIDDestinationOrder:
			if n, err := ReadUint32(o, v); err == nil {
				e.QoS.DestinationOrder = n
			}
		case PIDPartition:
			if s, err := readStrings(o, v); err == nil {
				e.QoS.Partition = s
			}
		case PIDUserData:
			if b, err := ReadOctets(o, v); err == nil {
				e.QoS.UserData = append([]byte(nil), b...)
			}
		case PIDTopicData:
			if b, err := ReadOctets(o, v); err == nil {
				e.QoS.TopicData = append([]byte(nil), b...)
			}
		case PIDGroupData:
			if b, err := ReadOctets(o, v); err == nil {
				e.QoS.GroupData = append([]byte(nil), b...)
			}
		case PIDTypeInformation:
			e.HasTypeInformation = true
		}
	}
	if !gotGUID {
		return e, errNoEndpointGUID
	}
	if e.Participant.IsZero() {
		e.Participant = e.GUID.Prefix
	}
	return e, nil
}

var errNoEndpointGUID = errorf("rtps: endpoint data without an endpoint GUID")

// readStrings reads a sequence<string>.
func readStrings(o binary.ByteOrder, b []byte) ([]string, error) {
	r := cdrReader{o: o, b: b}
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if n > maxProperties {
		return nil, errorf("rtps: partition of too many names")
	}
	out := make([]string, 0, n)
	for range n {
		s, err := r.string()
		if err != nil {
			return out, err
		}
		out = append(out, s)
	}
	return out, nil
}
