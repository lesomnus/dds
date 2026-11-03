package cdr

import "reflect"

func maxAlignOf(v reflect.Type) int {
	// iterate struct fields to find max alignment
	n := 1
	switch v.Kind() {
	case reflect.Array:
		n = max(n, maxAlignOf(v.Elem()))
	case reflect.Slice:
		n = max(n, maxAlignOf(v.Elem()), 4)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			n = max(n, maxAlignOf(v.Field(i).Type))
		}
	default:
		switch v.Kind() {
		case reflect.Bool, reflect.Int8, reflect.Uint8:
			n = max(n, 1)
		case reflect.Int16, reflect.Uint16:
			n = max(n, 2)
		case reflect.Int32, reflect.Uint32, reflect.Float32:
			n = max(n, 4)
		case reflect.Int64, reflect.Uint64, reflect.Float64:
			n = max(n, 8)
		}
	}
	return n
}
