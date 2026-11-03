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
			if !ok || string(m[:4]) != "RTPS" || sp != 51234 || dp != 7400 {
				t.Errorf("%s: payload %q %d %d %v", c.name, m, sp, dp, ok)
			}
		}
	}
}
