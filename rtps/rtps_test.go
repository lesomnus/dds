package rtps_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/dds/rtps"
)

// The fixtures were sent by ROS 2 Jazzy nodes on Cyclone DDS 0.10.5
// (rmw_cyclonedds_cpp) in domain 77, and received by cmd/ddsspy.

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// spdpData returns the one SPDP DATA submessage of a message.
func spdpData(t *testing.T, b []byte) (rtps.Header, rtps.Submessage, rtps.Data) {
	t.Helper()
	m, err := rtps.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range m.Submessages {
		if s.ID != rtps.SubData {
			continue
		}
		d, err := rtps.ParseData(s)
		if err != nil {
			t.Fatal(err)
		}
		if d.Writer == rtps.EntitySPDPWriter {
			return m.Header, s, d
		}
	}
	t.Fatal("no SPDP DATA")
	return rtps.Header{}, rtps.Submessage{}, rtps.Data{}
}

func TestParseCycloneSPDPAnnouncement(t *testing.T) {
	h, _, d := spdpData(t, fixture(t, "spdp-talker.rtps"))
	if h.Vendor != rtps.VendorCyclone {
		t.Errorf("vendor %s, want cyclonedds", h.Vendor)
	}
	if d.Payload == nil || d.KeyOnly {
		t.Fatal("an announcement carries its data")
	}
	pl, err := rtps.ReadParameterListPayload(d.Payload)
	if err != nil {
		t.Fatal(err)
	}
	p, err := rtps.ParseParticipantData(pl, h.Vendor)
	if err != nil {
		t.Fatal(err)
	}
	if p.GUID.Prefix != h.Prefix || p.GUID.Entity != rtps.EntityParticipant {
		t.Errorf("GUID %s, want %s.%s", p.GUID, h.Prefix, rtps.EntityParticipant)
	}
	if p.LeaseDuration != 10*time.Second {
		t.Errorf("lease %s, want Cyclone's default 10s", p.LeaseDuration)
	}
	for k, want := range map[string]string{"__ProcessName": "talker", "__Pid": "7"} {
		if p.Properties[k] != want {
			t.Errorf("%s = %q, want %q", k, p.Properties[k], want)
		}
	}
	if p.Properties["__Hostname"] == "" {
		t.Error("no __Hostname")
	}
	if string(p.UserData) != "enclave=/;" {
		t.Errorf("user data %q, want ROS 2's enclave", p.UserData)
	}
	if len(p.MetatrafficUnicast) == 0 || len(p.DefaultUnicast) == 0 {
		t.Errorf("locators: metatraffic %v, default %v", p.MetatrafficUnicast, p.DefaultUnicast)
	}
	if want := p.Properties["__Hostname"] + "/0.10.5/Linux/Linux"; p.CycloneInternals != want {
		t.Errorf("Cyclone internals %q, want %q", p.CycloneInternals, want)
	}
	const sedp = rtps.BuiltinPublicationsAnnouncer | rtps.BuiltinPublicationsDetector |
		rtps.BuiltinSubscriptionsAnnouncer | rtps.BuiltinSubscriptionsDetector
	if p.BuiltinEndpoints&sedp != sedp {
		t.Errorf("builtin endpoints %#x lack SEDP", p.BuiltinEndpoints)
	}
}

func TestParseCycloneSPDPDispose(t *testing.T) {
	h, s, d := spdpData(t, fixture(t, "spdp-dispose-talker.rtps"))
	if s.Flags&rtps.DataFlagKey == 0 || !d.KeyOnly {
		t.Errorf("flags %#x: a dispose carries the key only", s.Flags)
	}
	v, ok := d.InlineQoS.Get(rtps.PIDStatusInfo)
	if !ok {
		t.Fatal("no status info")
	}
	st, err := rtps.ReadStatusInfo(v)
	if err != nil {
		t.Fatal(err)
	}
	if st != rtps.StatusDisposed|rtps.StatusUnregistered {
		t.Errorf("status %#x, want disposed|unregistered", st)
	}
	pl, err := rtps.ReadParameterListPayload(d.Payload)
	if err != nil {
		t.Fatal(err)
	}
	g, ok := pl.Get(rtps.PIDParticipantGUID)
	if !ok {
		t.Fatal("the key is the participant's GUID")
	}
	guid, _ := rtps.ReadGUID(g)
	if guid.Prefix != h.Prefix {
		t.Errorf("disposes %s, sent by %s", guid.Prefix, h.Prefix)
	}
}

func TestParseRejects(t *testing.T) {
	for name, b := range map[string][]byte{
		"empty":     nil,
		"short":     []byte("RTPS"),
		"not rtps":  []byte("HTTP/1.1 200 OK\r\n\r\n...."),
		"version 3": append([]byte("RTPS\x03\x00\x01\x10"), make([]byte, 12)...),
	} {
		if _, err := rtps.Parse(b); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestSubmessageLengthIsBounded(t *testing.T) {
	b := fixture(t, "spdp-talker.rtps")
	b = b[:len(b)-1] // the last submessage now claims one byte too many
	if _, err := rtps.Parse(b); err != rtps.ErrTruncated {
		t.Errorf("err = %v, want ErrTruncated", err)
	}
}

// FuzzParse feeds anything through every decoder discovery uses: none may
// panic or read out of bounds, whatever the bytes.
func FuzzParse(f *testing.F) {
	for _, n := range []string{"spdp-talker.rtps", "spdp-composition.rtps", "spdp-dispose-talker.rtps"} {
		f.Add(fixture(f, n))
	}
	for i, d := range capture(f) {
		if i%8 == 0 { // a sample of everything Cyclone sent: SEDP, HEARTBEAT, ACKNACK, GAP, user data
			f.Add(d.Data)
		}
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, _ := rtps.Parse(b)
		for _, s := range m.Submessages {
			switch s.ID {
			case rtps.SubData:
				d, err := rtps.ParseData(s)
				if err != nil {
					continue
				}
				_, _ = d.InlineQoS.Get(rtps.PIDStatusInfo)
				if d.Payload != nil {
					if pl, err := rtps.ReadParameterListPayload(d.Payload); err == nil {
						_, _ = rtps.ParseParticipantData(pl, m.Header.Vendor)
						_, _ = rtps.ParseParticipantData(pl, rtps.VendorCyclone)
						_, _ = rtps.ParseEndpointData(rtps.Writer, pl)
						_, _ = rtps.ParseEndpointData(rtps.Reader, pl)
					}
				}
			case rtps.SubDataFrag:
				_, _ = rtps.ParseDataFrag(s)
			case rtps.SubHeartbeat:
				_, _ = rtps.ParseHeartbeat(s)
			case rtps.SubAckNack:
				if a, err := rtps.ParseAckNack(s); err == nil {
					_ = a.State.Members()
				}
			case rtps.SubGap:
				if g, err := rtps.ParseGap(s); err == nil {
					_ = g.Irrelevant(g.Start)
				}
			case rtps.SubInfoTS:
				_, _ = rtps.ParseInfoTS(s)
			case rtps.SubInfoDst:
				_, _ = rtps.ParseInfoDst(s)
			}
		}
	})
}
