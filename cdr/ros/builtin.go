package ros

import "time"

type Duration struct {
	Sec     int32
	Nanosec uint32
}

func (Duration) From(d time.Duration) Duration {
	sec := d / time.Second
	nsec := d % time.Second
	return Duration{
		Sec:     int32(sec),
		Nanosec: uint32(nsec),
	}
}

type Time struct {
	Sec  int32
	Nsec uint32
}

func Now() Time {
	return Time{}.FromTime(time.Now())
}

func (Time) FromTime(t time.Time) Time {
	return Time{
		Sec:  int32(t.Unix()),
		Nsec: uint32(t.Nanosecond()),
	}
}

func (t Time) ToTime() time.Time {
	return time.Unix(int64(t.Sec), int64(t.Nsec))
}
