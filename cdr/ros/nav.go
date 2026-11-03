package ros

type Odometry struct {
	Header       Header
	ChildFrameId string
	Pose         PoseWithCovariance
	Twist        TwistWithCovariance
}
