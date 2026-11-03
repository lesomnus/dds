package cdr_test

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/lesomnus/cdr"
)

func TestMarshal(t *testing.T) {
	for _, tc := range dataset {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.debug {
				runtime.Breakpoint()
			}

			bs, err := cdr.Marshal(tc.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(tc.bytes, bs[4:]) {
				t.Fatalf("invalid marshal result:\nwant: % X\ngot:  % X", tc.bytes, bs[4:])
			}
		})
	}
}
