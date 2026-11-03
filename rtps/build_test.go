package rtps

import "testing"

func TestBuildGapDataFragHeartbeatFrag(t *testing.T) {
	r, w := EntityID{0, 0, 1, 4}, EntityID{0, 0, 1, 3}
	var list SequenceNumberSet
	list.Base = 9
	list.Add(10)
	frags := []byte("0123456789")
	b := NewBuilder(GUIDPrefix{1}, VendorUnknown).
		Gap(r, w, 5, list).
		DataFrag(r, w, 7, 1, 2, 4, 10, nil, frags[:8]).
		HeartbeatFrag(r, w, 7, 3, 2).
		Bytes()
	m, err := Parse(b)
	if err != nil || len(m.Submessages) != 3 {
		t.Fatalf("%v %d", err, len(m.Submessages))
	}
	g, err := ParseGap(m.Submessages[0])
	if err != nil || g.Start != 5 || g.List.Base != 9 || !g.List.Has(10) || !g.Irrelevant(8) || g.Irrelevant(9) {
		t.Errorf("gap %+v %v", g, err)
	}
	f, err := ParseDataFrag(m.Submessages[1])
	if err != nil || f.SN != 7 || f.FragmentStart != 1 || f.FragmentsInSub != 2 || f.FragmentSize != 4 || f.SampleSize != 10 || string(f.Fragments[:8]) != "01234567" {
		t.Errorf("frag %+v %v", f, err)
	}
	h, err := ParseHeartbeatFrag(m.Submessages[2])
	if err != nil || h.SN != 7 || h.LastFragment != 3 || h.Count != 2 {
		t.Errorf("hbfrag %+v %v", h, err)
	}
}
