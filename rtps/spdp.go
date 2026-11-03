package rtps

import (
	"encoding/binary"
	"errors"
	"time"
)

// ParticipantData is what a participant announces of itself over SPDP
// (SPDPdiscoveredParticipantData, RTPS 2.5, 8.5.3.2 and 9.6.2.2.2).
type ParticipantData struct {
	GUID     GUID
	Version  ProtocolVersion
	Vendor   VendorID
	DomainID *uint32
	// DomainTag partitions a domain; empty is the default domain.
	DomainTag string

	// LeaseDuration is how long the participant may go unheard before it is
	// taken to be gone. The specification's default is 100 s.
	LeaseDuration time.Duration

	MetatrafficUnicast   []Locator
	MetatrafficMulticast []Locator
	DefaultUnicast       []Locator
	DefaultMulticast     []Locator

	// BuiltinEndpoints is the set of built-in endpoints the participant has
	// (BuiltinEndpointSet_t bits).
	BuiltinEndpoints uint32
	ExpectsInlineQoS bool

	// UserData is the participant's USER_DATA QoS. ROS 2 puts its enclave
	// there ("enclave=/;").
	UserData []byte
	// EntityName is the participant's name, if it sent one.
	EntityName string
	// Properties is the PROPERTY QoS. Cyclone DDS sends __Hostname, __Pid and
	// __ProcessName here.
	Properties map[string]string

	// CycloneInternals is Cyclone DDS's version string
	// ("<host>/<version>/<os>/<os>"), when the sender is Cyclone.
	CycloneInternals string
}

// DefaultLeaseDuration is what a participant that sends no lease duration
// gets (RTPS 2.5, 9.6.2.2.2).
const DefaultLeaseDuration = 100 * time.Second

// Built-in endpoint set bits (RTPS 2.5, 9.3.2.12).
const (
	BuiltinParticipantAnnouncer     = 1 << 0
	BuiltinParticipantDetector      = 1 << 1
	BuiltinPublicationsAnnouncer    = 1 << 2
	BuiltinPublicationsDetector     = 1 << 3
	BuiltinSubscriptionsAnnouncer   = 1 << 4
	BuiltinSubscriptionsDetector    = 1 << 5
	BuiltinParticipantMessageWriter = 1 << 10
	BuiltinParticipantMessageReader = 1 << 11
	BuiltinTopicsAnnouncer          = 1 << 28
	BuiltinTopicsDetector           = 1 << 29
)

// ParseParticipantData reads SPDP participant data out of a parameter list.
// vendor is the sender's (from the message header): it says what
// vendor-specific parameters mean. A parameter that does not decode is
// skipped, not fatal, unless it is the GUID.
func ParseParticipantData(pl ParameterList, vendor VendorID) (ParticipantData, error) {
	o := pl.Order
	d := ParticipantData{Vendor: vendor, LeaseDuration: DefaultLeaseDuration}
	gotGUID := false
	for _, p := range pl.Params {
		v := p.Value
		switch p.ID {
		case PIDParticipantGUID:
			g, err := ReadGUID(v)
			if err != nil {
				return d, err
			}
			d.GUID, gotGUID = g, true
		case PIDProtocolVersion:
			if len(v) >= 2 {
				d.Version = ProtocolVersion{v[0], v[1]}
			}
		case PIDVendorID:
			if len(v) >= 2 {
				d.Vendor = VendorID{v[0], v[1]}
			}
		case PIDDomainID:
			if id, err := ReadUint32(o, v); err == nil {
				d.DomainID = &id
			}
		case PIDDomainTag:
			if s, err := ReadString(o, v); err == nil {
				d.DomainTag = s
			}
		case PIDParticipantLease:
			if t, err := ReadDuration(o, v); err == nil {
				d.LeaseDuration = t
			}
		case PIDMetatrafficUnicastLoc:
			if l, err := ReadLocator(o, v); err == nil {
				d.MetatrafficUnicast = append(d.MetatrafficUnicast, l)
			}
		case PIDMetatrafficMulticastLc:
			if l, err := ReadLocator(o, v); err == nil {
				d.MetatrafficMulticast = append(d.MetatrafficMulticast, l)
			}
		case PIDDefaultUnicastLoc:
			if l, err := ReadLocator(o, v); err == nil {
				d.DefaultUnicast = append(d.DefaultUnicast, l)
			}
		case PIDDefaultMulticastLoc:
			if l, err := ReadLocator(o, v); err == nil {
				d.DefaultMulticast = append(d.DefaultMulticast, l)
			}
		case PIDBuiltinEndpointSet:
			if n, err := ReadUint32(o, v); err == nil {
				d.BuiltinEndpoints = n
			}
		case PIDExpectsInlineQoS:
			d.ExpectsInlineQoS = len(v) > 0 && v[0] != 0
		case PIDUserData:
			if b, err := ReadOctets(o, v); err == nil {
				d.UserData = append([]byte(nil), b...)
			}
		case PIDEntityName:
			if s, err := ReadString(o, v); err == nil {
				d.EntityName = s
			}
		case PIDPropertyList:
			if m, err := ReadProperties(o, v); err == nil {
				d.Properties = m
			}
		case PIDCycloneParticipantVersionInfo:
			if vendor == VendorCyclone {
				d.CycloneInternals = readCycloneVersionInfo(o, v)
			}
		}
	}
	if !gotGUID {
		return d, errNoGUID
	}
	return d, nil
}

var errNoGUID = errors.New("rtps: participant data without a GUID")

// readCycloneVersionInfo reads Cyclone's participant version info: a
// version, flags, three unused words, and a string naming host, Cyclone
// version and OS.
func readCycloneVersionInfo(o binary.ByteOrder, b []byte) string {
	if len(b) < 20 {
		return ""
	}
	r := cdrReader{o: o, b: b, at: 20}
	s, err := r.string()
	if err != nil {
		return ""
	}
	return s
}
