# ROS 2 Jazzy with Cyclone DDS (rmw_cyclonedds_cpp, Cyclone 0.10.x) -- the
# stack the E2E tests interoperate with -- and nodes to observe:
#   demo_nodes_cpp   talker, listener: one node per process
#   composition      manual_composition: several nodes in one process
FROM ros:jazzy-ros-base

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ros-jazzy-rmw-cyclonedds-cpp \
		ros-jazzy-demo-nodes-cpp \
		ros-jazzy-composition \
	&& rm -rf /var/lib/apt/lists/*

ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
