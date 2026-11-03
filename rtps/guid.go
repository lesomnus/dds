// Package rtps reads and writes DDSI-RTPS (OMG DDS Interoperability Wire
// Protocol, v2.x) messages: the header, the submessages, and the parameter
// lists discovery data is carried in.
//
// Everything here works on byte slices it is given and copies out what it
// keeps; nothing is read past a length it was told, whatever a packet claims.
package rtps

import (
	"encoding/hex"
	"fmt"
)

// GUIDPrefix identifies a participant. Every entity it contains shares it.
type GUIDPrefix [12]byte

func (p GUIDPrefix) String() string { return hex.EncodeToString(p[:]) }

// IsZero reports whether the prefix is unknown (all zeroes).
func (p GUIDPrefix) IsZero() bool { return p == GUIDPrefix{} }

// EntityID identifies an entity within a participant: a three-byte key and a
// kind.
type EntityID [4]byte

func (e EntityID) String() string { return hex.EncodeToString(e[:]) }

// Kind is the entity kind octet.
func (e EntityID) Kind() byte { return e[3] }

// GUID identifies an entity in a domain.
type GUID struct {
	Prefix GUIDPrefix
	Entity EntityID
}

func (g GUID) String() string { return fmt.Sprintf("%s.%s", g.Prefix, g.Entity) }

// Well-known entity IDs (RTPS 2.5, 9.3.1.4).
var (
	EntityUnknown     = EntityID{0x00, 0x00, 0x00, 0x00}
	EntityParticipant = EntityID{0x00, 0x00, 0x01, 0xc1}

	EntitySPDPWriter = EntityID{0x00, 0x01, 0x00, 0xc2}
	EntitySPDPReader = EntityID{0x00, 0x01, 0x00, 0xc7}

	EntitySEDPPublicationsWriter  = EntityID{0x00, 0x00, 0x03, 0xc2}
	EntitySEDPPublicationsReader  = EntityID{0x00, 0x00, 0x03, 0xc7}
	EntitySEDPSubscriptionsWriter = EntityID{0x00, 0x00, 0x04, 0xc2}
	EntitySEDPSubscriptionsReader = EntityID{0x00, 0x00, 0x04, 0xc7}

	EntityParticipantMessageWriter = EntityID{0x00, 0x02, 0x00, 0xc2}
	EntityParticipantMessageReader = EntityID{0x00, 0x02, 0x00, 0xc7}
)

// VendorID identifies the implementation that sent a message (RTPS 2.5,
// 8.3.3.1.3; the assigned values are kept by the OMG).
type VendorID [2]byte

func (v VendorID) String() string {
	if n, ok := vendorNames[v]; ok {
		return n
	}
	return hex.EncodeToString(v[:])
}

var (
	VendorRTI        = VendorID{0x01, 0x01}
	VendorOpenSplice = VendorID{0x01, 0x02}
	VendorFastDDS    = VendorID{0x01, 0x0f}
	VendorCyclone    = VendorID{0x01, 0x10}
	VendorUnknown    = VendorID{0x00, 0x00}
)

var vendorNames = map[VendorID]string{
	VendorRTI:        "rti",
	VendorOpenSplice: "opensplice",
	VendorFastDDS:    "fastdds",
	VendorCyclone:    "cyclonedds",
}

// ProtocolVersion is the RTPS version a message claims.
type ProtocolVersion struct{ Major, Minor byte }

func (v ProtocolVersion) String() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }
