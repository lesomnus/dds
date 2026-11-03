package ros

type Point struct {
	X float64
	Y float64
	Z float64
}

type Vector3 Point

type Quaternion struct {
	X float64
	Y float64
	Z float64
	W float64
}

type Pose struct {
	Position    Point
	Orientation Quaternion
}

type PoseStamped struct {
	Header Header
	Pose   Pose
}

type PoseWithCovariance struct {
	Pose       Pose
	Covariance [36]float64
}

type PoseWithCovarianceStamped struct {
	Header Header
	Pose   PoseWithCovariance
}

type se3 struct {
	Linear  Vector3
	Angular Vector3
}

type Accel se3
type Joint se3
type Twist se3

type AccelStamped struct {
	Header Header
	Accels []Accel
}

type TwistStamped struct {
	Header Header
	Twist  Twist
}

type TwistWithCovariance struct {
	Twist      Twist
	Covariance [36]float64
}
