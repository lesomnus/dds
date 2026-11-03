package ros

type Header struct {
	Stamp   Time
	FrameId string
}

type MultiArrayDimension struct {
	Label  string
	Size   uint32
	Stride uint32
}
type MultiArrayLayout struct {
	Dim        []MultiArrayDimension
	DataOffset uint32
}

type MultiArray[T any] struct {
	Layout MultiArrayLayout
	Data   []T
}
type Int8MultiArray = MultiArray[int8]
type Float32MultiArray = MultiArray[float32]

type SetBoolRequest struct {
	Data bool
}

type SetBoolResponse struct {
	Success bool
	Message string
}
