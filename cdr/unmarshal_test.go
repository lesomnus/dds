package cdr_test

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/lesomnus/cdr"
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
