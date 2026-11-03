package cdr_test

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/lesomnus/dds/cdr"
)

func TestUnmarshal(t *testing.T) {
	for _, tc := range dataset {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.debug {
				runtime.Breakpoint()
			}

			bs := append([]byte{0x00, 0x01, 0x00, 0x00}, tc.bytes...)
			v := reflect.New(reflect.TypeOf(tc.value)).Interface()
			err := cdr.Unmarshal(bs, v)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(tc.value, reflect.ValueOf(v).Elem().Interface()) {
				t.Fatalf("invalid unmarshal result:\nwant: %#v\ngot:  %#v", tc.value, reflect.ValueOf(v).Elem().Interface())
			}
		})
	}
}

// TestALengthBeyondTheDataIsRefused: a sequence claiming more elements than
// the bytes left can hold is an error, not an allocation of that size.
func TestALengthBeyondTheDataIsRefused(t *testing.T) {
	type S struct{ Items []struct{ A, B uint32 } }
	// CDR_LE header, then a length of 0x10000000 and four bytes.
	b := []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 1, 2, 3, 4}
	var v S
	if err := cdr.Unmarshal(b, &v); err == nil {
		t.Fatal("no error")
	}
}
