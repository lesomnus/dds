package cdr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
)

type Unmarshaler struct{}

func (u *Unmarshaler) Unmarshal(data []byte, v any) error {
	if len(data) < 4 {
		return io.ErrUnexpectedEOF
	}

	u_ := unmarshaler{}
	switch data[1] {
	case 0:
		u_.o = binary.BigEndian
	case 1:
		u_.o = binary.LittleEndian
	default:
		return errors.New("invalid byte order")
	}

	u_.b = data[4:]
	return u_.unmarshalValue(reflect.ValueOf(v))
}

func Unmarshal(data []byte, v any) error {
	u := Unmarshaler{}
	return u.Unmarshal(data, v)
}

type unmarshaler struct {
	b []byte
	p int
	o binary.ByteOrder
}

func (u *unmarshaler) align(n int) error {
	r := u.p % n
	p := (n - r) % n
	if p > len(u.b)-u.p {
		return io.ErrUnexpectedEOF
	}

	u.p += p
	return nil
}

func (u *unmarshaler) next(n int) ([]byte, error) {
	if n > len(u.b)-u.p {
		return nil, io.ErrUnexpectedEOF
	}

	k := u.p + n
	b := u.b[u.p:k]
	u.p = k
	return b, nil
}

func (u *unmarshaler) alignedNext(n int) ([]byte, error) {
	if err := u.align(n); err != nil {
		return nil, err
	}

	b, err := u.next(n)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (u *unmarshaler) unmarshalValue(v reflect.Value) error {
	kind := v.Kind()
	if kind == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return u.unmarshalValue(v.Elem())
	}

	switch kind {
	case reflect.Bool:
		if w, err := u.readBool(); err != nil {
			return err
		} else {
			v.SetBool(w)
		}
	case reflect.Int8:
		if w, err := u.readInt8(); err != nil {
			return err
		} else {
			v.SetInt(int64(w))
		}
	case reflect.Int16:
		if w, err := u.readInt16(); err != nil {
			return err
		} else {
			v.SetInt(int64(w))
		}
	case reflect.Int32:
		if w, err := u.readInt32(); err != nil {
			return err
		} else {
			v.SetInt(int64(w))
		}
	case reflect.Int64:
		if w, err := u.readInt64(); err != nil {
			return err
		} else {
			v.SetInt(w)
		}
	case reflect.Uint8:
		if w, err := u.readUint8(); err != nil {
			return err
		} else {
			v.SetUint(uint64(w))
		}
	case reflect.Uint16:
		if w, err := u.readUint16(); err != nil {
			return err
		} else {
			v.SetUint(uint64(w))
		}
	case reflect.Uint32:
		if w, err := u.readUint32(); err != nil {
			return err
		} else {
			v.SetUint(uint64(w))
		}
	case reflect.Uint64:
		if w, err := u.readUint64(); err != nil {
			return err
		} else {
			v.SetUint(w)
		}
	case reflect.Float32:
		if w, err := u.readFloat32(); err != nil {
			return err
		} else {
			v.SetFloat(float64(w))
		}
	case reflect.Float64:
		if w, err := u.readFloat64(); err != nil {
			return err
		} else {
			v.SetFloat(w)
		}
	case reflect.String:
		if w, err := u.readString(); err != nil {
			return err
		} else {
			v.SetString(w)
		}
	case reflect.Slice:
		if err := u.unmarshalSlice(v); err != nil {
			return err
		}
	case reflect.Array:
		if err := u.unmarshalArray(v); err != nil {
			return err
		}
	case reflect.Struct:
		if err := u.unmarshalStruct(v); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported type: %v", v.Kind())
	}

	return nil
}

func (u *unmarshaler) readBool() (bool, error) {
	b, err := u.alignedNext(1)
	if err != nil {
		return false, err
	}

	return b[0] != 0, nil
}

func (u *unmarshaler) readInt8() (int8, error) {
	b, err := u.alignedNext(1)
	if err != nil {
		return 0, err
	}

	return int8(b[0]), nil
}

func (u *unmarshaler) readUint8() (uint8, error) {
	b, err := u.alignedNext(1)
	if err != nil {
		return 0, err
	}

	return uint8(b[0]), nil
}

func (u *unmarshaler) readInt16() (int16, error) {
	b, err := u.alignedNext(2)
	if err != nil {
		return 0, err
	}

	return int16(u.o.Uint16(b)), nil
}

func (u *unmarshaler) readUint16() (uint16, error) {
	b, err := u.alignedNext(2)
	if err != nil {
		return 0, err
	}

	return u.o.Uint16(b), nil
}

func (u *unmarshaler) readInt32() (int32, error) {
	b, err := u.alignedNext(4)
	if err != nil {
		return 0, err
	}

	return int32(u.o.Uint32(b)), nil
}

func (u *unmarshaler) readUint32() (uint32, error) {
	b, err := u.alignedNext(4)
	if err != nil {
		return 0, err
	}

	return u.o.Uint32(b), nil
}

func (u *unmarshaler) readInt64() (int64, error) {
	b, err := u.alignedNext(8)
	if err != nil {
		return 0, err
	}

	return int64(u.o.Uint64(b)), nil
}

func (u *unmarshaler) readUint64() (uint64, error) {
	b, err := u.alignedNext(8)
	if err != nil {
		return 0, err
	}

	return u.o.Uint64(b), nil
}

func (u *unmarshaler) readFloat32() (float32, error) {
	b, err := u.alignedNext(4)
	if err != nil {
		return 0, err
	}

	return math.Float32frombits(u.o.Uint32(b)), nil
}

func (u *unmarshaler) readFloat64() (float64, error) {
	b, err := u.alignedNext(8)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(u.o.Uint64(b)), nil
}

func (u *unmarshaler) readString() (string, error) {
	l, err := u.readUint32()
	if err != nil {
		return "", err
	}
	if l == 0 {
		return "", nil
	}
	if l == 1 {
		// There is only a zero terminator.
		u.p += 1
		return "", nil
	}

	b, err := u.next(int(l - 1)) // skip zero terminator
	if err != nil {
		return "", err
	}
	// Consider skipped zero terminator as well.
	u.p += 1

	return string(b), nil
}

func (u *unmarshaler) unmarshalSlice(v reflect.Value) error {
	l, err := u.readUint32()
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}

	t := v.Type()
	s := 1
	kind := t.Elem().Kind()
	switch kind {
	case reflect.Bool:
		s = 1
	case reflect.Int8:
		s = 1
	case reflect.Uint8:
		s = 1
	case reflect.Int16:
		s = 2
	case reflect.Uint16:
		s = 2
	case reflect.Int32:
		s = 4
	case reflect.Uint32:
		s = 4
	case reflect.Int64:
		s = 8
	case reflect.Uint64:
		s = 8
	case reflect.Float32:
		s = 4
	case reflect.Float64:
		s = 8
	}
	if err := u.align(s); err != nil {
		return err
	}

	w := reflect.MakeSlice(v.Type(), int(l), int(l))
	for i := range int(l) {
		elem := w.Index(i)
		if err := u.unmarshalValue(elem); err != nil {
			return err
		}
	}

	v.Set(w)
	return nil
}

func (u *unmarshaler) unmarshalArray(v reflect.Value) error {
	for i := range v.Len() {
		elem := v.Index(i)
		if err := u.unmarshalValue(elem); err != nil {
			return err
		}
	}
	return nil
}

func (u *unmarshaler) unmarshalStruct(v reflect.Value) error {
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		if !t.Field(i).IsExported() {
			continue
		}
		if err := u.unmarshalValue(v.Field(i)); err != nil {
			return err
		}
	}
	return nil
}
