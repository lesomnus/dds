package traffic

import (
	"encoding/binary"
	"testing"

	"golang.org/x/net/bpf"
)

func ipv4UDP(payload []byte, fragOff uint16, proto byte, options int) []byte {
	ihl := 20 + options
	p := make([]byte, ihl+8+len(payload))
	p[0] = 0x40 | byte(ihl/4)
	binary.BigEndian.PutUint16(p[2:4], uint16(len(p)))
	binary.BigEndian.PutUint16(p[6:8], fragOff)
	p[8] = 64
	p[9] = proto
	copy(p[12:16], []byte{192, 168, 10, 2})
	copy(p[16:20], []byte{239, 255, 0, 1})
	binary.BigEndian.PutUint16(p[ihl:], 51234)
	binary.BigEndian.PutUint16(p[ihl+2:], 7400)
	binary.BigEndian.PutUint16(p[ihl+4:], uint16(8+len(payload)))
	copy(p[ihl+8:], payload)
	return p
}

func TestFilter(t *testing.T) {
	vm, err := bpf.NewVM(Filter(64))
	if err != nil {
		t.Fatal(err)
	}
	rtps := append([]byte("RTPS\x02\x01\x01\x10"), make([]byte, 200)...)
	for _, c := range []struct {
		name string
		p    []byte
		want int
	}{
		{"rtps", ipv4UDP(rtps, 0, 17, 0), 64},
		{"rtps, ip options", ipv4UDP(rtps, 0, 17, 8), 64},
		{"first fragment", ipv4UDP(rtps, 0x2000, 17, 0), 64},
		{"later fragment", ipv4UDP(rtps, 0x0010, 17, 0), 0},
		{"not rtps", ipv4UDP([]byte("HTTP/1.1 200 OK"), 0, 17, 0), 0},
		{"tcp", ipv4UDP(rtps, 0, 6, 0), 0},
		{"ipv6", append([]byte{0x60}, make([]byte, 60)...), 0},
		{"short", []byte{0x45, 0}, 0},
	} {
		got, err := vm.Run(c.p)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: kept %d, want %d", c.name, got, c.want)
		}
		if got > 0 {
			m, sp, dp, ok := rtpsPayload(c.p[:got])
			if !ok || string(m[:4]) != "RTPS" || sp.Port() != 51234 || dp.Port() != 7400 || sp.Addr().String() != "192.168.10.2" || dp.Addr().String() != "239.255.0.1" {
				t.Errorf("%s: payload %q %v %v %v", c.name, m, sp, dp, ok)
			}
		}
	}
}

func udpTo(dst [4]byte, port uint16, payload []byte) []byte {
	p := ipv4UDP(payload, 0, 17, 0)
	copy(p[16:20], dst[:])
	binary.BigEndian.PutUint16(p[22:24], port)
	return p
}

func TestSPDPFilter(t *testing.T) {
	vm, err := bpf.NewVM(SPDPFilter(SPDPSnaplen))
	if err != nil {
		t.Fatal(err)
	}
	rtps := append([]byte("RTPS\x02\x01\x01\x10"), make([]byte, 600)...)
	group := [4]byte{239, 255, 0, 1}
	for _, c := range []struct {
		name string
		p    []byte
		keep bool
	}{
		{"domain 0", udpTo(group, 7400, rtps), true},
		{"domain 138", udpTo(group, 41900, rtps), true},
		{"domain 232", udpTo(group, 7400+250*232, rtps), true},
		{"user multicast port", udpTo(group, 41901, rtps), false},
		{"unicast", udpTo([4]byte{192, 168, 10, 33}, 41900, rtps), false},
		{"another group", udpTo([4]byte{239, 255, 0, 2}, 41900, rtps), false},
		{"below the base", udpTo(group, 7399, rtps), false},
		{"not rtps", udpTo(group, 41900, []byte("hello, world, this is not it")), false},
	} {
		got, err := vm.Run(c.p)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if (got > 0) != c.keep {
			t.Errorf("%s: kept %d", c.name, got)
		}
	}
}

func TestSPDPDomain(t *testing.T) {
	for port, want := range map[uint16]int{7400: 0, 41900: 138, 7400 + 250*232: 232, 7401: -1, 7399: -1, 65535: -1} {
		d, ok := SPDPDomain(port)
		if (want < 0 && ok) || (want >= 0 && (!ok || d != uint32(want))) {
			t.Errorf("port %d: %d %v", port, d, ok)
		}
	}
}
