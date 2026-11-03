package cdr

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

type Marshaler struct {
	Order binary.ByteOrder
}

func (m *Marshaler) Marshal(v any) ([]byte, error) {
	m_ := marshaler{make([]byte, 0, 256), m.Order}
	if err := m_.marshal(reflect.ValueOf(v)); err != nil {
		return nil, err
	}

	return m_.b, nil
}

func Marshal(v any) ([]byte, error) {
	m := Marshaler{binary.LittleEndian}
	return m.Marshal(v)
}

type marshaler struct {
	b []byte
	o binary.ByteOrder
}

func (m *marshaler) marshal(v reflect.Value) error {
	if m.o == binary.BigEndian {
		m.b = append(m.b, []byte{0, 0, 0, 0}...)
	} else {
		m.b = append(m.b, []byte{0, 1, 0, 0}...)
	}

	m.marshalValue(v)
	m.alignedNextN(4, 0)
	return nil
}

func (m *marshaler) alignFrom(offset, a int) int {
	l := len(m.b) - 4 // skip header
	l += offset
	r := l % a
	p := (a - r) % a
	return p
}

func (m *marshaler) alignedNext(n int) []byte {
	return m.alignedNextN(n, n)
}

func (m *marshaler) alignedNextN(a, n int) []byte {
	k := n
	if a > 1 {
		k += m.alignFrom(0, a)
	}
	if cap(m.b)-len(m.b) < k {
		c := cap(m.b)
		for c < len(m.b)+k {
			c *= 2
		}

		b := make([]byte, len(m.b), c)
		copy(b, m.b)
		m.b = b
	}

	m.b = m.b[:len(m.b)+k]
	return m.b[len(m.b)-n:]
}

func (m *marshaler) marshalValue(v reflect.Value) {
	kind := v.Kind()
	if kind == reflect.Ptr {
		if v.IsNil() {
			// TODO: fill zeros
			return
		}

		m.marshalValue(v.Elem())
		return
	}

	switch kind {
	case reflect.Bool:
		m.marshalBool(v.Bool())
	case reflect.Int8:
		m.marshalInt8(int8(v.Int()))
	case reflect.Int16:
		m.marshalInt16(int16(v.Int()))
	case reflect.Int32:
		m.marshalInt32(int32(v.Int()))
	case reflect.Int64:
		m.marshalInt64(v.Int())
	case reflect.Uint8:
		m.marshalUint8(uint8(v.Uint()))
	case reflect.Uint16:
		m.marshalUint16(uint16(v.Uint()))
	case reflect.Uint32:
		m.marshalUint32(uint32(v.Uint()))
	case reflect.Uint64:
		m.marshalUint64(v.Uint())
	case reflect.Float32:
		m.marshalFloat32(float32(v.Float()))
	case reflect.Float64:
		m.marshalFloat64(v.Float())
	case reflect.String:
		m.marshalString(v.String())
	case reflect.Slice:
		m.marshalSlice(v)
	case reflect.Array:
		m.marshalArray(v)
	case reflect.Struct:
		m.marshalStruct(v)
	default:
		panic(fmt.Errorf("unsupported type: %v", v.Kind()))
	}
}

func (m *marshaler) marshalBool(v bool) {
	if v {
		m.b = append(m.b, 0x01)
	} else {
		m.b = append(m.b, 0x00)
	}
}

func (m *marshaler) marshalInt8(v int8) {
	m.b = append(m.b, byte(v))
}

func (m *marshaler) marshalUint8(v uint8) {
	m.b = append(m.b, v)
}

func (m *marshaler) marshalInt16(v int16) {
	b := m.alignedNext(2)
	m.o.PutUint16(b, uint16(v))
}

func (m *marshaler) marshalUint16(v uint16) {
	b := m.alignedNext(2)
	m.o.PutUint16(b, uint16(v))
}

func (m *marshaler) marshalInt32(v int32) {
	b := m.alignedNext(4)
	m.o.PutUint32(b, uint32(v))
}

func (m *marshaler) marshalUint32(v uint32) {
	b := m.alignedNext(4)
	m.o.PutUint32(b, uint32(v))
}

func (m *marshaler) marshalInt64(v int64) {
	b := m.alignedNext(8)
	m.o.PutUint64(b, uint64(v))
}

func (m *marshaler) marshalUint64(v uint64) {
	b := m.alignedNext(8)
	m.o.PutUint64(b, uint64(v))
}

func (m *marshaler) marshalFloat32(v float32) {
	b := m.alignedNext(4)
	m.o.PutUint32(b, math.Float32bits(v))
}

func (m *marshaler) marshalFloat64(v float64) {
	b := m.alignedNext(8)
	m.o.PutUint64(b, math.Float64bits(v))
}

func (m *marshaler) marshalString(v string) {
	l := len(v) + 1
	b := m.alignedNextN(4, 4+l)
	m.o.PutUint32(b[:4], uint32(l))
	copy(b[4:], v)
	b[len(b)-1] = 0
}

func (m *marshaler) marshalSlice(v reflect.Value) {
	if v.Len() == 0 {
		m.alignedNext(4)
		return
	}

	s := 0
	f := func(b []byte, w reflect.Value) {}

	kind := v.Type().Elem().Kind()
	switch kind {
	case reflect.Bool:
		s = 1
		f = func(b []byte, w reflect.Value) {
			if w.Bool() {
				b[0] = 0x01
			} else {
				b[0] = 0x00
			}
		}
	case reflect.Int8:
		s = 1
		f = func(b []byte, w reflect.Value) {
			b[0] = byte(w.Int())
		}
	case reflect.Uint8:
		s = 1
		f = func(b []byte, w reflect.Value) {
			b[0] = byte(w.Uint())
		}
	case reflect.Int16:
		s = 2
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint16(b, uint16(w.Int()))
		}
	case reflect.Uint16:
		s = 2
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint16(b, uint16(w.Uint()))
		}
	case reflect.Int32:
		s = 4
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint32(b, uint32(w.Int()))
		}
	case reflect.Uint32:
		s = 4
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint32(b, uint32(w.Uint()))
		}
	case reflect.Int64:
		s = 8
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint64(b, uint64(w.Int()))
		}
	case reflect.Uint64:
		s = 8
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint64(b, uint64(w.Uint()))
		}
	case reflect.Float32:
		s = 4
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint32(b, math.Float32bits(float32(w.Float())))
		}
	case reflect.Float64:
		s = 8
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint64(b, math.Float64bits(w.Float()))
		}
	case reflect.String:
		b := m.alignedNext(4)
		m.o.PutUint32(b[:4], uint32(v.Len()))
		for i := range v.Len() {
			elem := v.Index(i)
			m.marshalString(elem.String())
		}
		return
	// case reflect.Slice:
	// 	return m.marshalSlice(v)
	// case reflect.Array:
	// 	return m.marshalArray(v)
	case reflect.Struct:
		b := m.alignedNext(4)
		m.o.PutUint32(b[:4], uint32(v.Len()))
		for i := range v.Len() {
			elem := v.Index(i)
			m.marshalStruct(elem)
		}
		return
	default:
		f := func(v reflect.Value) {}
		switch kind {
		case reflect.String:
			f = func(v reflect.Value) {
				m.marshalString(v.String())
			}
		// case reflect.Slice:
		// 	return m.marshalSlice(v)
		// case reflect.Array:
		// 	return m.marshalArray(v)
		case reflect.Struct:
			f = m.marshalStruct
		default:
			panic("unsupported type in slice")
		}

		b := m.alignedNext(4)
		m.o.PutUint32(b[:4], uint32(v.Len()))
		for i := range v.Len() {
			elem := v.Index(i)
			f(elem)
		}
		return
	}

	// Assume there is 1-byte element the beginning,
	// so padded to 4-byte align for "length". -> `p_len` = 3
	//
	//	| x 0 0 0 |
	//	| l l l l |
	//	| e  ...  |
	//

	// `4` is size of length field.
	p_len := m.alignFrom(0, 4)
	offset := p_len + 4
	offset += m.alignFrom(offset, s) - p_len

	l := offset + s*v.Len()
	b := m.alignedNextN(4, l)
	m.o.PutUint32(b[:4], uint32(v.Len()))

	for i := range v.Len() {
		elem := v.Index(i)
		f(b[offset:offset+s], elem)
		offset += s
	}
}

func (m *marshaler) marshalArray(v reflect.Value) {
	s := 0
	f := func(b []byte, w reflect.Value) {}

	kind := v.Type().Elem().Kind()
	switch kind {
	case reflect.Bool:
		s = 1
		f = func(b []byte, w reflect.Value) {
			if w.Bool() {
				b[0] = 0x01
			} else {
				b[0] = 0x00
			}
		}
	case reflect.Int8:
		s = 1
		f = func(b []byte, w reflect.Value) {
			b[0] = byte(w.Int())
		}
	case reflect.Uint8:
		s = 1
		f = func(b []byte, w reflect.Value) {
			b[0] = byte(w.Uint())
		}
	case reflect.Int16:
		s = 2
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint16(b, uint16(w.Int()))
		}
	case reflect.Uint16:
		s = 2
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint16(b, uint16(w.Uint()))
		}
	case reflect.Int32:
		s = 4
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint32(b, uint32(w.Int()))
		}
	case reflect.Uint32:
		s = 4
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint32(b, uint32(w.Uint()))
		}
	case reflect.Int64:
		s = 8
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint64(b, uint64(w.Int()))
		}
	case reflect.Uint64:
		s = 8
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint64(b, uint64(w.Uint()))
		}
	case reflect.Float32:
		s = 4
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint32(b, math.Float32bits(float32(w.Float())))
		}
	case reflect.Float64:
		s = 8
		f = func(b []byte, w reflect.Value) {
			m.o.PutUint64(b, math.Float64bits(w.Float()))
		}
	default:
		a := 4
		f := func(v reflect.Value) {}
		switch kind {
		case reflect.String:
			f = func(v reflect.Value) {
				m.marshalString(v.String())
			}
		// case reflect.Slice:
		// 	return m.marshalSlice(v)
		// case reflect.Array:
		// 	return m.marshalArray(v)
		case reflect.Struct:
			a = maxAlignOf(v.Type().Elem())
			f = m.marshalStruct
		default:
			panic("unsupported type in slice")
		}

		m.alignedNextN(a, 0)
		for i := range v.Len() {
			elem := v.Index(i)
			f(elem)
		}

		return
	}

	l := s * v.Len()
	b := m.alignedNextN(s, l)

	offset := 0
	for i := range v.Len() {
		elem := v.Index(i)
		f(b[offset:offset+s], elem)
		offset += s
	}
}

func (m *marshaler) marshalStruct(v reflect.Value) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		if !t.Field(i).IsExported() {
			continue // unexported
		}
		m.marshalValue(v.Field(i))
	}
}
