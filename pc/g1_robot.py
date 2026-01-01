#!/usr/bin/env python3
"""
High-level G1 Robot Interface using the ZMQ Bridge.

This module provides a user-friendly API for controlling the G1 humanoid robot
through the ZMQ-to-DDS bridge.

Usage:
    from g1_robot import G1Robot

    robot = G1Robot("192.168.1.100")
    robot.connect()
    robot.boot()
    robot.set_velocity(0.5, 0, 0)  # Walk forward
    time.sleep(5)
    robot.stop()
    robot.damp()
    robot.disconnect()
"""

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from .g1_bridge_client import G1BridgeClient, BridgeError

logger = logging.getLogger(__name__)


class RobotState:
    """Container for robot state data."""

    def __init__(self):
        self.connected = False
        self.booted = False
        self.locomotion_stream_active = False

        # Latest sensor data
        self.low_state: Optional[Dict[str, Any]] = None
        self.odometry: Optional[Dict[str, Any]] = None
        self.sport_mode_state: Optional[Dict[str, Any]] = None

        # IMU quick access
        self.imu: Optional[Dict[str, Any]] = None
        self.motor_states: Optional[List[Dict[str, Any]]] = None

        # Timestamps
        self.last_low_state_time: float = 0
        self.last_odometry_time: float = 0

        self._lock = threading.Lock()

    def update_low_state(self, data: Dict[str, Any]):
        """Update from rt/lowstate topic."""
        with self._lock:
            self.low_state = data.get("data")
            self.last_low_state_time = data.get("timestamp", time.time())

            if self.low_state:
                self.imu = self.low_state.get("imu_state")
                self.motor_states = self.low_state.get("motor_state")

    def update_odometry(self, data: Dict[str, Any]):
        """Update from odometry topic."""
        with self._lock:
            self.odometry = data.get("data")
            self.last_odometry_time = data.get("timestamp", time.time())

    def update_sport_mode(self, data: Dict[str, Any]):
        """Update from sport mode state topic."""
        with self._lock:
            self.sport_mode_state = data.get("data")

    def get_position(self) -> Optional[Dict[str, float]]:
        """Get current position from odometry."""
        with self._lock:
            if self.odometry:
                pose = self.odometry.get("pose", {}).get("pose", {})
                position = pose.get("position", {})
                return {
                    "x": position.get("x", 0),
                    "y": position.get("y", 0),
                    "z": position.get("z", 0)
                }
            return None

    def get_orientation(self) -> Optional[Dict[str, float]]:
        """Get current orientation (quaternion) from odometry."""
        with self._lock:
            if self.odometry:
                pose = self.odometry.get("pose", {}).get("pose", {})
                orientation = pose.get("orientation", {})
                return {
                    "x": orientation.get("x", 0),
                    "y": orientation.get("y", 0),
                    "z": orientation.get("z", 0),
                    "w": orientation.get("w", 1)
                }
            return None

    def get_imu_rpy(self) -> Optional[Dict[str, float]]:
        """Get IMU roll/pitch/yaw in radians."""
        with self._lock:
            if self.imu:
                rpy = self.imu.get("rpy", [0, 0, 0])
                if isinstance(rpy, list) and len(rpy) >= 3:
                    return {"roll": rpy[0], "pitch": rpy[1], "yaw": rpy[2]}
            return None

    def get_motor_position(self, joint_index: int) -> Optional[float]:
        """Get motor position by joint index."""
        with self._lock:
            if self.motor_states and 0 <= joint_index < len(self.motor_states):
                return self.motor_states[joint_index].get("q")
            return None


class G1Robot:
    """
    High-level interface for controlling the G1 humanoid robot.

    Manages:
    - Connection lifecycle
    - Boot sequence
    - Locomotion stream (for continuous movement)
    - Topic subscriptions for state feedback
    - Clean shutdown
    """

    # Default locomotion stream parameters
    LOCOMOTION_STREAM_ID = "locomotion"
    LOCOMOTION_HZ = 50

    # Topic names
    TOPIC_LOW_STATE = "rt/lowstate"
    TOPIC_ODOMETRY = "rt/utlidar/robot_odom"
    TOPIC_SPORT_MODE = "rt/sportmodestate"

    def __init__(
        self,
        bridge_ip: str,
        rep_port: int = 5556,
        pub_port: int = 5555,
        auto_subscribe: bool = True,
        state_hz: float = 10
    ):
        """
        Initialize the G1 robot interface.

        Args:
            bridge_ip: IP address of the bridge server
            rep_port: Port for commands
            pub_port: Port for topic data
            auto_subscribe: Automatically subscribe to state topics on boot
            state_hz: Rate for state topic subscriptions
        """
        self._client = G1BridgeClient(bridge_ip, rep_port, pub_port)
        self._auto_subscribe = auto_subscribe
        self._state_hz = state_hz

        self.state = RobotState()

        # Current velocity (for tracking)
        self._current_vx = 0.0
        self._current_vy = 0.0
        self._current_vyaw = 0.0

    @property
    def connected(self) -> bool:
        """Check if connected to bridge."""
        return self._client.connected

    @property
    def booted(self) -> bool:
        """Check if robot is booted and ready."""
        return self.state.booted

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(self) -> bool:
        """
        Connect to the bridge server.

        Returns:
            True if connection successful

        Raises:
            BridgeError: If connection fails
        """
        result = self._client.connect()
        self.state.connected = True

        # Register internal callbacks
        self._client.on_topic(self.TOPIC_LOW_STATE, self.state.update_low_state)
        self._client.on_topic(self.TOPIC_ODOMETRY, self.state.update_odometry)
        self._client.on_topic(self.TOPIC_SPORT_MODE, self.state.update_sport_mode)

        logger.info("Connected to G1 robot")
        return result

    def disconnect(self):
        """Disconnect from the bridge server."""
        if self.state.locomotion_stream_active:
            try:
                self.stop_locomotion()
            except BridgeError:
                pass

        self._client.disconnect()
        self.state.connected = False
        self.state.booted = False
        logger.info("Disconnected from G1 robot")

    # =========================================================================
    # Boot Sequence
    # =========================================================================

    def boot(self, subscribe_topics: bool = True) -> bool:
        """
        Boot the robot for locomotion.

        This:
        1. Calls Start() to enter locomotion mode
        2. Creates the locomotion stream
        3. Optionally subscribes to state topics

        Args:
            subscribe_topics: Whether to subscribe to state topics

        Returns:
            True if boot successful

        Raises:
            BridgeError: If boot fails
        """
        if not self.connected:
            raise BridgeError("Not connected to bridge", code="NOT_CONNECTED")

        logger.info("Booting G1 robot...")

        # Start locomotion mode
        self._client.call("Start")
        time.sleep(0.5)  # Allow mode transition

        # Create locomotion stream with zero velocity
        self._client.stream_create(
            self.LOCOMOTION_STREAM_ID,
            "Move",
            {"vx": 0, "vy": 0, "vyaw": 0, "continuous": True},
            hz=self.LOCOMOTION_HZ
        )
        self.state.locomotion_stream_active = True

        # Register and subscribe to topics if requested
        if subscribe_topics and self._auto_subscribe:
            # Register and subscribe to low state (always available)
            try:
                self._client.register_topic(self.TOPIC_LOW_STATE, "LowState")
                self._client.subscribe(self.TOPIC_LOW_STATE, hz=self._state_hz)
            except BridgeError as e:
                logger.warning(f"Could not subscribe to {self.TOPIC_LOW_STATE}: {e}")

            # Register and subscribe to odometry (may not be available if LiDAR off)
            try:
                self._client.register_topic(self.TOPIC_ODOMETRY, "Odometry")
                self._client.subscribe(self.TOPIC_ODOMETRY, hz=self._state_hz)
            except BridgeError as e:
                logger.warning(f"Could not subscribe to {self.TOPIC_ODOMETRY}: {e}")

        self.state.booted = True
        logger.info("G1 robot booted successfully")
        return True

    def shutdown(self):
        """
        Safely shutdown the robot.

        This:
        1. Stops all movement
        2. Puts robot in damping mode
        3. Stops the locomotion stream
        """
        logger.info("Shutting down G1 robot...")

        # Stop movement first
        try:
            self.stop()
            time.sleep(0.2)
        except BridgeError:
            pass

        # Damp the robot
        try:
            self.damp()
        except BridgeError:
            pass

        # Stop locomotion stream
        self.stop_locomotion()

        self.state.booted = False
        logger.info("G1 robot shutdown complete")

    def stop_locomotion(self):
        """Stop the locomotion stream."""
        if self.state.locomotion_stream_active:
            try:
                self._client.stream_stop(self.LOCOMOTION_STREAM_ID)
            except BridgeError:
                pass
            self.state.locomotion_stream_active = False

    def emergency_stop(self):
        """
        Emergency stop: immediately stop all streams and damp the robot.

        This is a safety command that bypasses normal shutdown sequence.
        Use in emergencies when robot needs to stop immediately.
        """
        logger.warning("EMERGENCY STOP activated!")
        self._client.stop_all()
        self.state.locomotion_stream_active = False
        self.state.booted = False
        self._current_vx = 0
        self._current_vy = 0
        self._current_vyaw = 0

    # =========================================================================
    # Movement Commands
    # =========================================================================

    def set_velocity(self, vx: float, vy: float, vyaw: float):
        """
        Set robot velocity.

        Args:
            vx: Forward velocity (m/s, positive = forward)
            vy: Lateral velocity (m/s, positive = left)
            vyaw: Angular velocity (rad/s, positive = counter-clockwise)

        Raises:
            BridgeError: If command fails
        """
        if not self.state.locomotion_stream_active:
            raise BridgeError("Locomotion stream not active. Call boot() first.",
                            code="NOT_BOOTED")

        self._client.stream_update(
            self.LOCOMOTION_STREAM_ID,
            {"vx": vx, "vy": vy, "vyaw": vyaw}
        )

        self._current_vx = vx
        self._current_vy = vy
        self._current_vyaw = vyaw

    def move_forward(self, speed: float = 0.3):
        """Move forward at given speed (m/s)."""
        self.set_velocity(speed, 0, 0)

    def move_backward(self, speed: float = 0.3):
        """Move backward at given speed (m/s)."""
        self.set_velocity(-speed, 0, 0)

    def move_left(self, speed: float = 0.2):
        """Strafe left at given speed (m/s)."""
        self.set_velocity(0, speed, 0)

    def move_right(self, speed: float = 0.2):
        """Strafe right at given speed (m/s)."""
        self.set_velocity(0, -speed, 0)

    def turn_left(self, speed: float = 0.5):
        """Turn left at given angular speed (rad/s)."""
        self.set_velocity(0, 0, speed)

    def turn_right(self, speed: float = 0.5):
        """Turn right at given angular speed (rad/s)."""
        self.set_velocity(0, 0, -speed)

    def stop(self):
        """Stop all movement."""
        if self.state.locomotion_stream_active:
            self.set_velocity(0, 0, 0)
        else:
            self._client.call("StopMove")

        self._current_vx = 0
        self._current_vy = 0
        self._current_vyaw = 0

    # =========================================================================
    # Posture Commands
    # =========================================================================

    def damp(self):
        """
        Put robot in damping mode.

        Motors provide passive resistance but no active control.
        Use this before shutting down or when not actively controlling.
        """
        self._client.call("Damp")
        logger.info("Robot in damping mode")

    def stand_up(self):
        """Stand up from lying position."""
        self._client.call("StandUp")
        logger.info("Standing up...")

    def sit(self):
        """Sit down."""
        self._client.call("Sit")
        logger.info("Sitting down...")

    def high_stand(self):
        """Stand at maximum height."""
        self._client.call("HighStand")
        logger.info("High stand")

    def low_stand(self):
        """Stand at minimum height."""
        self._client.call("LowStand")
        logger.info("Low stand")

    def set_stand_height(self, height: float):
        """
        Set standing height.

        Args:
            height: Stand height value
        """
        self._client.call("SetStandHeight", {"height": height})

    def zero_torque(self):
        """
        Set zero torque mode.

        WARNING: Robot will collapse! Use only when robot is supported.
        """
        self._client.call("ZeroTorque")
        logger.warning("Robot in zero torque mode!")

    # =========================================================================
    # Arm Gestures
    # =========================================================================

    def wave_hand(self, turn: bool = False):
        """
        Wave hand gesture.

        Args:
            turn: If True, use alternate wave animation
        """
        self._client.call("WaveHand", {"turn_flag": turn})
        logger.info("Waving hand...")

    def shake_hand(self, stage: int = -1):
        """
        Shake hand gesture.

        Args:
            stage: Handshake stage (-1 = auto toggle, 0 = first, 1 = second)
        """
        self._client.call("ShakeHand", {"stage": stage})
        logger.info("Shaking hand...")

    # =========================================================================
    # Topic Subscriptions
    # =========================================================================

    def register_topic(self, topic: str, msg_type: str):
        """
        Register a topic with a message type before subscribing.

        Args:
            topic: Topic name (e.g., "rt/lowstate")
            msg_type: Message type name (e.g., "LowState")

        Example:
            robot.register_topic("rt/bms_state", "BmsState")
            robot.subscribe("rt/bms_state", hz=1)
        """
        self._client.register_topic(topic, msg_type)

    def subscribe(self, topic: str, hz: float = 10):
        """
        Subscribe to a registered DDS topic.

        Note: Topic must be registered first with register_topic().

        Args:
            topic: Topic name
            hz: Forward rate
        """
        self._client.subscribe(topic, hz)

    def unsubscribe(self, topic: str):
        """
        Unsubscribe from a topic.

        Args:
            topic: Topic name
        """
        self._client.unsubscribe(topic)

    def list_known_topics(self) -> Dict[str, Dict[str, str]]:
        """
        List known G1 topics for reference.

        Returns:
            Dict with topic info including type and description
        """
        return self._client.list_known_topics()

    def list_msg_types(self) -> List[str]:
        """
        List available message types.

        Returns:
            List of message type names for use with register_topic()
        """
        return self._client.list_msg_types()

    def on_topic(self, topic: str, callback: Callable[[Dict[str, Any]], None]):
        """
        Register a callback for topic data.

        Args:
            topic: Topic name
            callback: Function to call with data
        """
        self._client.on_topic(topic, callback)

    def on_heartbeat(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Register a callback for heartbeat messages.

        Args:
            callback: Function to call with heartbeat data
        """
        self._client.on_heartbeat(callback)

    # =========================================================================
    # State Accessors
    # =========================================================================

    def get_position(self) -> Optional[Dict[str, float]]:
        """Get current position {x, y, z} in meters."""
        return self.state.get_position()

    def get_orientation(self) -> Optional[Dict[str, float]]:
        """Get current orientation quaternion {x, y, z, w}."""
        return self.state.get_orientation()

    def get_imu_rpy(self) -> Optional[Dict[str, float]]:
        """Get IMU roll/pitch/yaw {roll, pitch, yaw} in radians."""
        return self.state.get_imu_rpy()

    def get_joint_position(self, joint_index: int) -> Optional[float]:
        """Get joint position by index in radians."""
        return self.state.get_motor_position(joint_index)

    def get_velocity(self) -> Dict[str, float]:
        """Get current commanded velocity."""
        return {
            "vx": self._current_vx,
            "vy": self._current_vy,
            "vyaw": self._current_vyaw
        }

    # =========================================================================
    # Utilities
    # =========================================================================

    def ping(self) -> Dict[str, Any]:
        """Ping the bridge server."""
        return self._client.ping()

    def list_topics(self) -> List[str]:
        """List available topics."""
        return self._client.list_topics()

    def list_methods(self) -> List[str]:
        """List available methods."""
        return self._client.list_methods()

    def status(self) -> Dict[str, Any]:
        """Get bridge status."""
        return self._client.status()

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


# =============================================================================
# Joint Index Constants (for reference)
# =============================================================================

class G1JointIndex:
    """Joint indices for G1 humanoid robot."""

    # Left leg
    LeftHipPitch = 0
    LeftHipRoll = 1
    LeftHipYaw = 2
    LeftKnee = 3
    LeftAnklePitch = 4
    LeftAnkleRoll = 5

    # Right leg
    RightHipPitch = 6
    RightHipRoll = 7
    RightHipYaw = 8
    RightKnee = 9
    RightAnklePitch = 10
    RightAnkleRoll = 11

    # Waist
    WaistYaw = 12
    WaistRoll = 13
    WaistPitch = 14

    # Left arm
    LeftShoulderPitch = 15
    LeftShoulderRoll = 16
    LeftShoulderYaw = 17
    LeftElbow = 18
    LeftWristRoll = 19
    LeftWristPitch = 20
    LeftWristYaw = 21

    # Right arm
    RightShoulderPitch = 22
    RightShoulderRoll = 23
    RightShoulderYaw = 24
    RightElbow = 25
    RightWristRoll = 26
    RightWristPitch = 27
    RightWristYaw = 28


# =============================================================================
# Convenience function
# =============================================================================

def create_robot(
    bridge_ip: str,
    rep_port: int = 5556,
    pub_port: int = 5555
) -> G1Robot:
    """
    Create and connect a G1Robot instance.

    Args:
        bridge_ip: IP address of the bridge server
        rep_port: Command port
        pub_port: Topic data port

    Returns:
        Connected G1Robot instance
    """
    robot = G1Robot(bridge_ip, rep_port, pub_port)
    robot.connect()
    return robot
