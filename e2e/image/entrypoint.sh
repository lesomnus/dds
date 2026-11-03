#!/bin/bash
# Run a command with the ROS environment sourced, as PID 1's child so that a
# SIGTERM to the container reaches it (a graceful DDS shutdown) and SIGKILL
# does not (a crash).
set -e
source /opt/ros/jazzy/setup.bash
exec "$@"
