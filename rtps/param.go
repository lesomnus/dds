package rtps

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"time"
)

// ParameterID identifies a parameter (RTPS 2.5, 9.6.2.2).
type ParameterID uint16

// Parameter IDs this package reads.
const (
	PIDPad                    ParameterID = 0x0000
	PIDSentinel               ParameterID = 0x0001
	PIDParticipantLease       ParameterID = 0x0002
	PIDDomainID               ParameterID = 0x000f
	PIDProtocolVersion        ParameterID = 0x0015
	PIDVendorID               ParameterID = 0x0016
	PIDUserData               ParameterID = 0x002c
	PIDDefaultUnicastLoc      ParameterID = 0x0031
	PIDMetatrafficUnicastLoc  ParameterID = 0x0032
	PIDMetatrafficMulticastLc ParameterID = 0x0033
	PIDExpectsInlineQoS       ParameterID = 0x0043
	PIDDefaultMulticastLoc    ParameterID = 0x0048
	PIDParticipantGUID        ParameterID = 0x0050
	PIDBuiltinEndpointSet     ParameterID = 0x0058
	PIDPropertyList           ParameterID = 0x0059
	PIDEntityName             ParameterID = 0x0062
	PIDKeyHash                ParameterID = 0x0070
	PIDStatusInfo             ParameterID = 0x0071
	PIDDomainTag              ParameterID = 0x4014

	// pidVendorSpecific marks an ID whose meaning belongs to the vendor that
	// sent it (RTPS 2.5, 9.6.2.2.1).
	pidVendorSpecific ParameterID = 0x8000
	// pidMustUnderstand marks an ID a receiver must not ignore.
	pidMustUnderstand ParameterID = 0x4000

	// Cyclone DDS (vendor 01.10): ddsi_adlink_participant_version_info.
	PIDCycloneParticipantVersionInfo ParameterID = 0x8007
)

// IsVendorSpecific reports whether the ID's meaning depends on the sender's
// vendor.
func (id ParameterID) IsVendorSpecific() bool { return id&pidVendorSpecific != 0 }

// Parameter is one parameter; Value aliases the list it came from.
type Parameter struct {
	ID    ParameterID
	Value []byte
}

// ParameterList is a list of parameters and the byte order of their values.
type ParameterList struct {
	Order  binary.ByteOrder
	Params []Parameter
}

// Get returns the first parameter with the ID.
func (pl ParameterList) Get(id ParameterID) ([]byte, bool) {
	for _, p := range pl.Params {
		if p.ID == id {
			return p.Value, true
		}
	}
	return nil, false
}

// All returns every parameter with the ID, in order.
func (pl ParameterList) All(id ParameterID) [][]byte {
	var out [][]byte
	for _, p := range pl.Params {
		if p.ID == id {
			out = append(out, p.Value)
		}
	}
	return out
}

var ErrNoSentinel = errors.New("rtps: parameter list without a sentinel")

// ReadParameterList reads a parameter list up to and including its sentinel,
// and returns how many bytes it took.
func ReadParameterList(o binary.ByteOrder, b []byte) (ParameterList, int, error) {
	pl := ParameterList{Order: o}
	at := 0
	for {
		if len(b)-at < 4 {
			return pl, at, ErrNoSentinel
		}
		id := ParameterID(o.Uint16(b[at : at+2]))
		n := int(o.Uint16(b[at+2 : at+4]))
		at += 4
		if id == PIDSentinel {
			return pl, at, nil
		}
		if n > len(b)-at {
			return pl, at, ErrTruncated
		}
		if id != PIDPad {
			pl.Params = append(pl.Params, Parameter{ID: id, Value: b[at : at+n : at+n]})
		}
		at += n
	}
}

// Encapsulation identifies how a serialized payload is represented (RTPS
// 2.5, 10.2; XTypes 1.3, 7.6.3.1.2).
type Encapsulation uint16

const (
	EncCDRBE   Encapsulation = 0x0000
	EncCDRLE   Encapsulation = 0x0001
	EncPLCDRBE Encapsulation = 0x0002
	EncPLCDRLE Encapsulation = 0x0003
)

// Order is the byte order of the encapsulated data.
func (e Encapsulation) Order() binary.ByteOrder {
	if e&1 != 0 {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

// SplitPayload reads a serialized payload's encapsulation header and returns
// the data after it.
func SplitPayload(b []byte) (Encapsulation, []byte, error) {
	if len(b) < 4 {
		return 0, nil, ErrTruncated
	}
	return Encapsulation(binary.BigEndian.Uint16(b[0:2])), b[4:], nil
}

// ReadParameterListPayload reads a serialized payload that holds a parameter
// list (PL_CDR_LE or PL_CDR_BE), as discovery data does.
func ReadParameterListPayload(b []byte) (ParameterList, error) {
	enc, data, err := SplitPayload(b)
	if err != nil {
		return ParameterList{}, err
	}
	if enc != EncPLCDRLE && enc != EncPLCDRBE {
		return ParameterList{}, fmt.Errorf("rtps: payload encapsulation %#04x is not a parameter list", uint16(enc))
	}
	pl, _, err := ReadParameterList(enc.Order(), data)
	return pl, err
}

// Value decoders. Each reads one parameter's value in the list's byte order.

// LocatorKind is the transport of a locator.
type LocatorKind int32

const (
	LocatorUDPv4 LocatorKind = 1
	LocatorUDPv6 LocatorKind = 2
)

// Locator is an address a participant or endpoint can be reached at.
type Locator struct {
	Kind LocatorKind
	Port uint32
	Addr netip.Addr
}

func (l Locator) String() string {
	return netip.AddrPortFrom(l.Addr, uint16(l.Port)).String()
}

// ReadLocator reads a Locator_t: kind, port, and a 16-byte address of which
// an IPv4 address takes the last four.
func ReadLocator(o binary.ByteOrder, b []byte) (Locator, error) {
	if len(b) < 24 {
		return Locator{}, ErrTruncated
	}
	l := Locator{Kind: LocatorKind(int32(o.Uint32(b[0:4]))), Port: o.Uint32(b[4:8])}
	switch l.Kind {
	case LocatorUDPv4:
		l.Addr = netip.AddrFrom4([4]byte(b[20:24]))
	case LocatorUDPv6:
		l.Addr = netip.AddrFrom16([16]byte(b[8:24]))
	default:
		l.Addr = netip.AddrFrom16([16]byte(b[8:24]))
	}
	return l, nil
}

// Duration values that mean "forever" and "none" on the wire.
const (
	DurationInfinite = time.Duration(1<<63 - 1)
)

// ReadDuration reads a Duration_t: seconds and a binary fraction.
func ReadDuration(o binary.ByteOrder, b []byte) (time.Duration, error) {
	if len(b) < 8 {
		return 0, ErrTruncated
	}
	sec := int32(o.Uint32(b[0:4]))
	frac := o.Uint32(b[4:8])
	if sec == 0x7fffffff && (frac == 0xffffffff || frac == 0x7fffffff) {
		return DurationInfinite, nil
	}
	return time.Duration(sec)*time.Second + time.Duration(uint64(frac)*uint64(time.Second)>>32), nil
}

// ReadUint32 reads a 32-bit unsigned value.
func ReadUint32(o binary.ByteOrder, b []byte) (uint32, error) {
	if len(b) < 4 {
		return 0, ErrTruncated
	}
	return o.Uint32(b[0:4]), nil
}

// ReadGUID reads a 16-byte GUID.
func ReadGUID(b []byte) (GUID, error) {
	if len(b) < 16 {
		return GUID{}, ErrTruncated
	}
	var g GUID
	copy(g.Prefix[:], b[0:12])
	copy(g.Entity[:], b[12:16])
	return g, nil
}

// cdrReader reads CDR strings and sequences within one parameter value,
// aligning relative to its start (parameter values start 4-aligned).
type cdrReader struct {
	o  binary.ByteOrder
	b  []byte
	at int
}

func (r *cdrReader) align(n int) error {
	if pad := (n - r.at%n) % n; pad > 0 {
		if pad > len(r.b)-r.at {
			return ErrTruncated
		}
		r.at += pad
	}
	return nil
}

func (r *cdrReader) uint32() (uint32, error) {
	if err := r.align(4); err != nil {
		return 0, err
	}
	if len(r.b)-r.at < 4 {
		return 0, ErrTruncated
	}
	v := r.o.Uint32(r.b[r.at:])
	r.at += 4
	return v, nil
}

// octets reads a sequence<octet>.
func (r *cdrReader) octets() ([]byte, error) {
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if int64(n) > int64(len(r.b)-r.at) {
		return nil, ErrTruncated
	}
	v := r.b[r.at : r.at+int(n)]
	r.at += int(n)
	return v, nil
}

// string reads a CDR string: a length that counts the terminating NUL, then
// the characters.
func (r *cdrReader) string() (string, error) {
	b, err := r.octets()
	if err != nil {
		return "", err
	}
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return string(b), nil
}

// ReadString reads a parameter holding a CDR string.
func ReadString(o binary.ByteOrder, b []byte) (string, error) {
	r := cdrReader{o: o, b: b}
	return r.string()
}

// ReadOctets reads a parameter holding a sequence<octet>.
func ReadOctets(o binary.ByteOrder, b []byte) ([]byte, error) {
	r := cdrReader{o: o, b: b}
	return r.octets()
}

// maxProperties bounds a property list a packet may claim, against a count
// that would have us loop on garbage.
const maxProperties = 1024

// ReadProperties reads a PID_PROPERTY_LIST value: a sequence of name/value
// string pairs. Binary properties, which may follow, are not read.
func ReadProperties(o binary.ByteOrder, b []byte) (map[string]string, error) {
	r := cdrReader{o: o, b: b}
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if n > maxProperties {
		return nil, fmt.Errorf("rtps: %d properties", n)
	}
	out := make(map[string]string, n)
	for range n {
		k, err := r.string()
		if err != nil {
			return out, err
		}
		v, err := r.string()
		if err != nil {
			return out, err
		}
		out[k] = v
	}
	return out, nil
}

// StatusInfo flags (RTPS 2.5, 9.6.4.9): what a DATA without data says about
// its instance.
const (
	StatusDisposed     = 0x1
	StatusUnregistered = 0x2
	StatusFiltered     = 0x4
)

// ReadStatusInfo reads PID_STATUS_INFO, whose four octets are big endian
// whatever the list's byte order.
func ReadStatusInfo(b []byte) (uint32, error) {
	if len(b) < 4 {
		return 0, ErrTruncated
	}
	return binary.BigEndian.Uint32(b[0:4]), nil
}
