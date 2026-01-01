#!/usr/bin/env python3
"""
ZMQ-to-DDS Bridge Server for Unitree G1 Humanoid Robot

This bridge runs on the Jetson inside the robot and allows a PC to control
the robot over WiFi without needing DDS configured on the PC.

The bridge is a "dumb pipe with timers":
- Executes commands it's told to execute
- Runs command streams at specified frequencies
- Forwards subscribed DDS topics to the PC
- Never crashes - always returns errors to client

Usage:
    python bridge_server.py [--rep-port 5556] [--pub-port 5555] [--interface eth0]
"""

import argparse
import json
import logging
import queue
import signal
import sys
import threading
import time
import traceback
from dataclasses import fields, is_dataclass
from typing import Any, Callable, Dict, List, Optional, Type

import zmq

# Unitree SDK imports
from unitree_sdk2py.core.channel import (
    ChannelFactoryInitialize,
    ChannelPublisher,
    ChannelSubscriber,
)
from unitree_sdk2py.g1.loco.g1_loco_client import LocoClient
from unitree_sdk2py.g1.loco.g1_loco_api import ROBOT_API_ID_LOCO_SET_SWING_HEIGHT

# IDL message types for G1 humanoid
from unitree_sdk2py.idl.unitree_hg.msg.dds_ import LowState_ as HG_LowState_
from unitree_sdk2py.idl.unitree_hg.msg.dds_ import LowCmd_ as HG_LowCmd_
from unitree_sdk2py.idl.unitree_hg.msg.dds_ import BmsState_ as HG_BmsState_
from unitree_sdk2py.idl.unitree_hg.msg.dds_ import HandState_ as HG_HandState_

# IDL message types for navigation/odometry
from unitree_sdk2py.idl.nav_msgs.msg.dds_ import Odometry_
from unitree_sdk2py.idl.std_msgs.msg.dds_ import String_

# IDL message types for GO series (SportModeState, WirelessController)
from unitree_sdk2py.idl.unitree_go.msg.dds_ import SportModeState_
from unitree_sdk2py.idl.unitree_go.msg.dds_ import WirelessController_

# IDL message types for point clouds (LiDAR)
from unitree_sdk2py.idl.sensor_msgs.msg.dds_ import PointCloud2_


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('bridge_server')


# =============================================================================
# Message Serialization
# =============================================================================

def dataclass_to_dict(obj: Any) -> Any:
    """
    Recursively convert a dataclass (or IDL message) to a JSON-serializable dict.
    Handles nested dataclasses, arrays, and primitive types.
    """
    if obj is None:
        return None

    if isinstance(obj, (int, float, str, bool)):
        return obj

    if isinstance(obj, bytes):
        return list(obj)

    if isinstance(obj, (list, tuple)):
        return [dataclass_to_dict(item) for item in obj]

    if hasattr(obj, '__iter__') and not isinstance(obj, (str, dict)):
        # Handle array types from cyclonedds
        try:
            return [dataclass_to_dict(item) for item in obj]
        except TypeError:
            return str(obj)

    if is_dataclass(obj) and not isinstance(obj, type):
        result = {}
        for field in fields(obj):
            value = getattr(obj, field.name)
            result[field.name] = dataclass_to_dict(value)
        return result

    # Fallback for other types
    try:
        return str(obj)
    except Exception:
        return None


# =============================================================================
# Topic Registry
# =============================================================================

class TopicRegistry:
    """
    Registry of known DDS topics and their message types.
    Pre-registers all supported topics for G1.
    """

    def __init__(self):
        self._topics: Dict[str, Type] = {}
        self._register_default_topics()

    def _register_default_topics(self):
        """Register all supported G1 topics."""
        # G1 Humanoid topics (unitree_hg)
        self.register("rt/lowstate", HG_LowState_)
        self.register("rt/lf/lowstate", HG_LowState_)  # Wireless controller variant
        self.register("rt/arm_sdk", HG_LowCmd_)  # Arm control
        self.register("rt/lowcmd", HG_LowCmd_)  # Low-level commands

        # Battery Management System
        self.register("rt/bms_state", HG_BmsState_)

        # Hand state topics (Dex3 hands)
        self.register("rt/left_hand/state", HG_HandState_)
        self.register("rt/right_hand/state", HG_HandState_)

        # Wireless controller input
        self.register("rt/wirelesscontroller", WirelessController_)

        # Navigation/Odometry
        self.register("rt/utlidar/robot_odom", Odometry_)
        self.register("rt/odom", Odometry_)

        # Sport mode state (GO series compatible)
        self.register("rt/sportmodestate", SportModeState_)

        # LiDAR topics
        self.register("rt/utlidar/cloud", PointCloud2_)
        self.register("rt/utlidar/switch", String_)

        logger.info(f"Registered {len(self._topics)} default topics")

    def register(self, topic_name: str, msg_type: Type):
        """Register a topic with its message type."""
        self._topics[topic_name] = msg_type
        logger.debug(f"Registered topic: {topic_name} -> {msg_type.__name__}")

    def get_type(self, topic_name: str) -> Optional[Type]:
        """Get the message type for a topic."""
        return self._topics.get(topic_name)

    def list_topics(self) -> List[str]:
        """List all registered topics."""
        return list(self._topics.keys())


# =============================================================================
# Stream Manager
# =============================================================================

class CommandStream:
    """
    A single command stream that runs at a specified frequency.
    Thread-safe parameter updates.
    """

    def __init__(
        self,
        stream_id: str,
        method: str,
        params: Dict[str, Any],
        hz: float,
        executor: Callable[[str, Dict[str, Any]], Any]
    ):
        self.stream_id = stream_id
        self.method = method
        self.hz = hz
        self.executor = executor

        self._params = params.copy()
        self._params_lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._error_count = 0
        self._success_count = 0
        self._last_error: Optional[str] = None

    def update_params(self, params: Dict[str, Any]):
        """Thread-safe parameter update."""
        with self._params_lock:
            self._params.update(params)

    def get_params(self) -> Dict[str, Any]:
        """Thread-safe parameter read."""
        with self._params_lock:
            return self._params.copy()

    def start(self):
        """Start the stream loop."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"Stream '{self.stream_id}' started: {self.method} at {self.hz}Hz")

    def stop(self):
        """Stop the stream loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
        logger.info(f"Stream '{self.stream_id}' stopped")

    def _run_loop(self):
        """Main loop that executes commands at the configured frequency."""
        interval = 1.0 / self.hz

        while self._running:
            loop_start = time.time()

            try:
                params = self.get_params()
                self.executor(self.method, params)
                self._success_count += 1
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                logger.warning(f"Stream '{self.stream_id}' error: {e}")
                # Don't stop - keep trying

            # Sleep for remaining interval
            elapsed = time.time() - loop_start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    def get_stats(self) -> Dict[str, Any]:
        """Get stream statistics."""
        return {
            "stream_id": self.stream_id,
            "method": self.method,
            "hz": self.hz,
            "params": self.get_params(),
            "running": self._running,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "last_error": self._last_error,
        }


class StreamManager:
    """Manages all command streams."""

    def __init__(self, executor: Callable[[str, Dict[str, Any]], Any]):
        self._streams: Dict[str, CommandStream] = {}
        self._lock = threading.Lock()
        self._executor = executor

    def create(
        self,
        stream_id: str,
        method: str,
        params: Dict[str, Any],
        hz: float
    ) -> bool:
        """Create and start a new stream."""
        with self._lock:
            if stream_id in self._streams:
                raise ValueError(f"Stream '{stream_id}' already exists")

            stream = CommandStream(stream_id, method, params, hz, self._executor)
            self._streams[stream_id] = stream
            stream.start()
            return True

    def update(self, stream_id: str, params: Dict[str, Any]) -> bool:
        """Update parameters of an existing stream."""
        with self._lock:
            if stream_id not in self._streams:
                raise ValueError(f"Stream '{stream_id}' not found")

            self._streams[stream_id].update_params(params)
            return True

    def stop(self, stream_id: str) -> bool:
        """Stop and remove a stream."""
        with self._lock:
            if stream_id not in self._streams:
                raise ValueError(f"Stream '{stream_id}' not found")

            self._streams[stream_id].stop()
            del self._streams[stream_id]
            return True

    def stop_all(self):
        """Stop all streams."""
        with self._lock:
            for stream in self._streams.values():
                stream.stop()
            self._streams.clear()

    def list_streams(self) -> List[str]:
        """List all active stream IDs."""
        with self._lock:
            return list(self._streams.keys())

    def get_stats(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a stream."""
        with self._lock:
            if stream_id in self._streams:
                return self._streams[stream_id].get_stats()
            return None

    def get_all_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for all streams."""
        with self._lock:
            return [s.get_stats() for s in self._streams.values()]


# =============================================================================
# Topic Subscription Manager
# =============================================================================

class TopicSubscription:
    """
    A subscription to a DDS topic with rate limiting for forwarding.
    """

    def __init__(
        self,
        topic_name: str,
        msg_type: Type,
        forward_hz: float,
        data_queue: queue.Queue
    ):
        self.topic_name = topic_name
        self.msg_type = msg_type
        self.forward_hz = forward_hz
        self._data_queue = data_queue
        self._subscriber: Optional[ChannelSubscriber] = None
        self._last_forward_time = 0.0
        self._forward_interval = 1.0 / forward_hz if forward_hz > 0 else 0
        self._message_count = 0
        self._forward_count = 0

    def start(self):
        """Start the DDS subscription."""
        try:
            self._subscriber = ChannelSubscriber(self.topic_name, self.msg_type)
            self._subscriber.Init(self._on_message, 10)
            logger.info(f"Subscribed to '{self.topic_name}' at {self.forward_hz}Hz forward rate")
        except Exception as e:
            logger.error(f"Failed to subscribe to '{self.topic_name}': {e}")
            raise

    def stop(self):
        """Stop the subscription."""
        # CycloneDDS doesn't have explicit unsubscribe, just drop the reference
        self._subscriber = None
        logger.info(f"Unsubscribed from '{self.topic_name}'")

    def _on_message(self, msg):
        """Callback when DDS message received."""
        self._message_count += 1

        # Rate limiting
        now = time.time()
        if self._forward_interval > 0:
            if (now - self._last_forward_time) < self._forward_interval:
                return

        self._last_forward_time = now
        self._forward_count += 1

        # Serialize and queue for forwarding
        try:
            data = {
                "type": "topic_data",
                "topic": self.topic_name,
                "timestamp": now,
                "data": dataclass_to_dict(msg)
            }
            self._data_queue.put_nowait(data)
        except queue.Full:
            logger.warning(f"Queue full, dropping message from '{self.topic_name}'")
        except Exception as e:
            logger.error(f"Error serializing message from '{self.topic_name}': {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get subscription statistics."""
        return {
            "topic": self.topic_name,
            "forward_hz": self.forward_hz,
            "message_count": self._message_count,
            "forward_count": self._forward_count,
        }


class SubscriptionManager:
    """Manages all topic subscriptions."""

    def __init__(self, topic_registry: TopicRegistry, data_queue: queue.Queue):
        self._subscriptions: Dict[str, TopicSubscription] = {}
        self._lock = threading.Lock()
        self._topic_registry = topic_registry
        self._data_queue = data_queue

    def subscribe(self, topic_name: str, forward_hz: float) -> bool:
        """Subscribe to a topic."""
        with self._lock:
            if topic_name in self._subscriptions:
                raise ValueError(f"Already subscribed to '{topic_name}'")

            msg_type = self._topic_registry.get_type(topic_name)
            if msg_type is None:
                raise ValueError(f"Unknown topic '{topic_name}'. Use list_topics to see available topics.")

            sub = TopicSubscription(topic_name, msg_type, forward_hz, self._data_queue)
            sub.start()
            self._subscriptions[topic_name] = sub
            return True

    def unsubscribe(self, topic_name: str) -> bool:
        """Unsubscribe from a topic."""
        with self._lock:
            if topic_name not in self._subscriptions:
                raise ValueError(f"Not subscribed to '{topic_name}'")

            self._subscriptions[topic_name].stop()
            del self._subscriptions[topic_name]
            return True

    def unsubscribe_all(self):
        """Unsubscribe from all topics."""
        with self._lock:
            for sub in self._subscriptions.values():
                sub.stop()
            self._subscriptions.clear()

    def list_subscriptions(self) -> List[str]:
        """List all active subscriptions."""
        with self._lock:
            return list(self._subscriptions.keys())

    def get_all_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for all subscriptions."""
        with self._lock:
            return [s.get_stats() for s in self._subscriptions.values()]


# =============================================================================
# Command Executor
# =============================================================================

class CommandExecutor:
    """
    Executes LocoClient commands.
    Maps method names to actual SDK calls.
    """

    def __init__(self):
        self._loco_client: Optional[LocoClient] = None

        # Map of method names to (handler, requires_params)
        self._methods: Dict[str, tuple] = {
            # State transitions (one-shot, no params)
            "Damp": (self._damp, False),
            "Start": (self._start, False),
            "StandUp": (self._stand_up, False),
            "Lie2StandUp": (self._lie2_stand_up, False),
            "Squat2StandUp": (self._squat2_stand_up, False),
            "StandUp2Squat": (self._stand_up2_squat, False),
            "Sit": (self._sit, False),
            "ZeroTorque": (self._zero_torque, False),
            "StopMove": (self._stop_move, False),
            "HighStand": (self._high_stand, False),
            "LowStand": (self._low_stand, False),

            # Parameterized commands
            "Move": (self._move, True),
            "SetVelocity": (self._set_velocity, True),
            "SetStandHeight": (self._set_stand_height, True),
            "SetSwingHeight": (self._set_swing_height, True),
            "BalanceStand": (self._balance_stand, True),
            "SetBalanceMode": (self._set_balance_mode, True),
            "WaveHand": (self._wave_hand, True),
            "ShakeHand": (self._shake_hand, True),
            "SetFsmId": (self._set_fsm_id, True),
            "SetTaskId": (self._set_task_id, True),
        }

    def init(self):
        """Initialize the LocoClient."""
        self._loco_client = LocoClient()
        self._loco_client.Init()
        logger.info("LocoClient initialized")

    def execute(self, method: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a command by method name.

        Args:
            method: The method name (e.g., "Move", "Damp")
            params: Optional parameters for the method

        Returns:
            Result of the command (usually return code)

        Raises:
            ValueError: If method is unknown
            Exception: If command fails
        """
        if method not in self._methods:
            raise ValueError(f"Unknown method '{method}'. Available: {list(self._methods.keys())}")

        handler, requires_params = self._methods[method]

        if requires_params and not params:
            raise ValueError(f"Method '{method}' requires parameters")

        if requires_params:
            return handler(params)
        else:
            return handler()

    def list_methods(self) -> List[str]:
        """List all available methods."""
        return list(self._methods.keys())

    # --- One-shot commands (no parameters) ---

    def _damp(self) -> int:
        return self._loco_client.Damp()

    def _start(self) -> int:
        return self._loco_client.Start()

    def _stand_up(self) -> int:
        return self._loco_client.Lie2StandUp()

    def _lie2_stand_up(self) -> int:
        return self._loco_client.Lie2StandUp()

    def _squat2_stand_up(self) -> int:
        return self._loco_client.Squat2StandUp()

    def _stand_up2_squat(self) -> int:
        return self._loco_client.StandUp2Squat()

    def _sit(self) -> int:
        return self._loco_client.Sit()

    def _zero_torque(self) -> int:
        return self._loco_client.ZeroTorque()

    def _stop_move(self) -> int:
        return self._loco_client.StopMove()

    def _high_stand(self) -> int:
        return self._loco_client.HighStand()

    def _low_stand(self) -> int:
        return self._loco_client.LowStand()

    # --- Parameterized commands ---

    def _move(self, params: Dict[str, Any]) -> int:
        vx = float(params.get("vx", 0))
        vy = float(params.get("vy", 0))
        vyaw = float(params.get("vyaw", 0))
        continuous = bool(params.get("continuous", True))
        return self._loco_client.Move(vx, vy, vyaw, continuous)

    def _set_velocity(self, params: Dict[str, Any]) -> int:
        vx = float(params.get("vx", 0))
        vy = float(params.get("vy", 0))
        omega = float(params.get("omega", 0))
        duration = float(params.get("duration", 1.0))
        return self._loco_client.SetVelocity(vx, vy, omega, duration)

    def _set_stand_height(self, params: Dict[str, Any]) -> int:
        height = float(params["height"])
        return self._loco_client.SetStandHeight(height)

    def _set_swing_height(self, params: Dict[str, Any]) -> int:
        """Set swing height using direct API call (not exposed in LocoClient)."""
        import json as _json
        height = float(params["height"])
        p = {"data": height}
        parameter = _json.dumps(p)
        code, _ = self._loco_client._Call(ROBOT_API_ID_LOCO_SET_SWING_HEIGHT, parameter)
        return code

    def _balance_stand(self, params: Dict[str, Any]) -> int:
        mode = int(params["mode"])
        return self._loco_client.BalanceStand(mode)

    def _set_balance_mode(self, params: Dict[str, Any]) -> int:
        mode = int(params["mode"])
        return self._loco_client.SetBalanceMode(mode)

    def _wave_hand(self, params: Dict[str, Any]) -> int:
        turn_flag = bool(params.get("turn_flag", False))
        return self._loco_client.WaveHand(turn_flag)

    def _shake_hand(self, params: Dict[str, Any]) -> int:
        stage = int(params.get("stage", -1))
        return self._loco_client.ShakeHand(stage)

    def _set_fsm_id(self, params: Dict[str, Any]) -> int:
        fsm_id = int(params["fsm_id"])
        return self._loco_client.SetFsmId(fsm_id)

    def _set_task_id(self, params: Dict[str, Any]) -> int:
        task_id = float(params["task_id"])
        return self._loco_client.SetTaskId(task_id)


# =============================================================================
# Bridge Server
# =============================================================================

class BridgeServer:
    """
    Main ZMQ-to-DDS bridge server.

    Handles:
    - ZMQ REQ-REP socket for commands (port 5556)
    - ZMQ PUB socket for topic data (port 5555)
    - Stream management
    - Topic subscriptions
    - Heartbeat publishing
    """

    def __init__(
        self,
        rep_port: int = 5556,
        pub_port: int = 5555,
        heartbeat_hz: float = 1.0
    ):
        self.rep_port = rep_port
        self.pub_port = pub_port
        self.heartbeat_hz = heartbeat_hz

        self._start_time = time.time()
        self._running = False

        # ZMQ context and sockets
        self._zmq_context: Optional[zmq.Context] = None
        self._rep_socket: Optional[zmq.Socket] = None
        self._pub_socket: Optional[zmq.Socket] = None

        # Components
        self._topic_registry = TopicRegistry()
        self._data_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._executor = CommandExecutor()
        self._stream_manager: Optional[StreamManager] = None
        self._subscription_manager: Optional[SubscriptionManager] = None

        # Threads
        self._pub_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

    def start(self, network_interface: Optional[str] = None):
        """
        Start the bridge server.

        Args:
            network_interface: Network interface for DDS (e.g., "eth0")
        """
        logger.info("Starting bridge server...")

        # Initialize DDS
        logger.info("Initializing DDS channel factory...")
        if network_interface:
            ChannelFactoryInitialize(0, network_interface)
        else:
            ChannelFactoryInitialize(0)

        # Initialize command executor
        self._executor.init()

        # Initialize managers
        self._stream_manager = StreamManager(self._executor.execute)
        self._subscription_manager = SubscriptionManager(
            self._topic_registry, self._data_queue
        )

        # Initialize ZMQ
        logger.info("Initializing ZMQ sockets...")
        self._zmq_context = zmq.Context()

        self._rep_socket = self._zmq_context.socket(zmq.REP)
        self._rep_socket.bind(f"tcp://*:{self.rep_port}")
        logger.info(f"REP socket bound to port {self.rep_port}")

        self._pub_socket = self._zmq_context.socket(zmq.PUB)
        self._pub_socket.bind(f"tcp://*:{self.pub_port}")
        logger.info(f"PUB socket bound to port {self.pub_port}")

        self._running = True

        # Start publisher thread
        self._pub_thread = threading.Thread(target=self._pub_loop, daemon=True)
        self._pub_thread.start()

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        logger.info("Bridge server started successfully")

        # Run command handler in main thread
        self._command_loop()

    def stop(self):
        """Stop the bridge server."""
        logger.info("Stopping bridge server...")
        self._running = False

        # Stop all streams and subscriptions
        if self._stream_manager:
            self._stream_manager.stop_all()
        if self._subscription_manager:
            self._subscription_manager.unsubscribe_all()

        # Close ZMQ sockets
        if self._rep_socket:
            self._rep_socket.close()
        if self._pub_socket:
            self._pub_socket.close()
        if self._zmq_context:
            self._zmq_context.term()

        logger.info("Bridge server stopped")

    def _command_loop(self):
        """Main loop for handling commands from ZMQ REP socket."""
        logger.info("Command handler ready")

        while self._running:
            try:
                # Wait for command with timeout
                if self._rep_socket.poll(timeout=1000):
                    message = self._rep_socket.recv_string()
                    response = self._handle_command(message)
                    self._rep_socket.send_string(json.dumps(response))
            except zmq.ZMQError as e:
                if self._running:
                    logger.error(f"ZMQ error in command loop: {e}")
            except Exception as e:
                logger.error(f"Error in command loop: {e}")
                traceback.print_exc()

    def _handle_command(self, message: str) -> Dict[str, Any]:
        """
        Handle a single command message.

        Returns a response dict with status and optional result/error.
        """
        try:
            cmd = json.loads(message)
        except json.JSONDecodeError as e:
            return {"status": "error", "error": f"Invalid JSON: {e}", "code": "INVALID_JSON"}

        cmd_type = cmd.get("cmd")

        try:
            if cmd_type == "ping":
                return self._handle_ping()

            elif cmd_type == "stream_create":
                return self._handle_stream_create(cmd)

            elif cmd_type == "stream_update":
                return self._handle_stream_update(cmd)

            elif cmd_type == "stream_stop":
                return self._handle_stream_stop(cmd)

            elif cmd_type == "subscribe":
                return self._handle_subscribe(cmd)

            elif cmd_type == "unsubscribe":
                return self._handle_unsubscribe(cmd)

            elif cmd_type == "call":
                return self._handle_call(cmd)

            elif cmd_type == "list_topics":
                return self._handle_list_topics()

            elif cmd_type == "list_methods":
                return self._handle_list_methods()

            elif cmd_type == "list_streams":
                return self._handle_list_streams()

            elif cmd_type == "list_subscriptions":
                return self._handle_list_subscriptions()

            elif cmd_type == "status":
                return self._handle_status()

            elif cmd_type == "stop_all":
                return self._handle_stop_all()

            else:
                return {
                    "status": "error",
                    "error": f"Unknown command: {cmd_type}",
                    "code": "UNKNOWN_COMMAND"
                }

        except ValueError as e:
            return {"status": "error", "error": str(e), "code": "INVALID_PARAMS"}
        except Exception as e:
            logger.error(f"Error handling command: {e}")
            traceback.print_exc()
            return {"status": "error", "error": str(e), "code": "INTERNAL_ERROR"}

    def _handle_ping(self) -> Dict[str, Any]:
        """Handle ping command."""
        return {
            "status": "ok",
            "uptime": time.time() - self._start_time
        }

    def _handle_stream_create(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """Handle stream_create command."""
        stream_id = cmd.get("stream_id")
        method = cmd.get("method")
        params = cmd.get("params", {})
        hz = cmd.get("hz", 50)

        if not stream_id:
            raise ValueError("stream_id is required")
        if not method:
            raise ValueError("method is required")

        self._stream_manager.create(stream_id, method, params, hz)
        return {"status": "ok", "stream_id": stream_id}

    def _handle_stream_update(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """Handle stream_update command."""
        stream_id = cmd.get("stream_id")
        params = cmd.get("params", {})

        if not stream_id:
            raise ValueError("stream_id is required")

        self._stream_manager.update(stream_id, params)
        return {"status": "ok", "stream_id": stream_id}

    def _handle_stream_stop(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """Handle stream_stop command."""
        stream_id = cmd.get("stream_id")

        if not stream_id:
            raise ValueError("stream_id is required")

        self._stream_manager.stop(stream_id)
        return {"status": "ok", "stream_id": stream_id}

    def _handle_subscribe(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """Handle subscribe command."""
        topic = cmd.get("topic")
        hz = cmd.get("hz", 10)

        if not topic:
            raise ValueError("topic is required")

        self._subscription_manager.subscribe(topic, hz)
        return {"status": "ok", "topic": topic}

    def _handle_unsubscribe(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """Handle unsubscribe command."""
        topic = cmd.get("topic")

        if not topic:
            raise ValueError("topic is required")

        self._subscription_manager.unsubscribe(topic)
        return {"status": "ok", "topic": topic}

    def _handle_call(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """Handle call command (one-shot)."""
        method = cmd.get("method")
        params = cmd.get("params")

        if not method:
            raise ValueError("method is required")

        result = self._executor.execute(method, params)
        return {"status": "ok", "result": result}

    def _handle_list_topics(self) -> Dict[str, Any]:
        """Handle list_topics command."""
        return {
            "status": "ok",
            "topics": self._topic_registry.list_topics()
        }

    def _handle_list_methods(self) -> Dict[str, Any]:
        """Handle list_methods command."""
        return {
            "status": "ok",
            "methods": self._executor.list_methods()
        }

    def _handle_list_streams(self) -> Dict[str, Any]:
        """Handle list_streams command."""
        return {
            "status": "ok",
            "streams": self._stream_manager.get_all_stats()
        }

    def _handle_list_subscriptions(self) -> Dict[str, Any]:
        """Handle list_subscriptions command."""
        return {
            "status": "ok",
            "subscriptions": self._subscription_manager.get_all_stats()
        }

    def _handle_status(self) -> Dict[str, Any]:
        """Handle status command."""
        return {
            "status": "ok",
            "uptime": time.time() - self._start_time,
            "streams": self._stream_manager.list_streams(),
            "subscriptions": self._subscription_manager.list_subscriptions(),
        }

    def _handle_stop_all(self) -> Dict[str, Any]:
        """
        Emergency stop: stop all streams and damp the robot.

        This is a safety command that:
        1. Stops all active command streams
        2. Calls Damp() to put robot in safe mode
        """
        errors = []

        # Stop all streams
        try:
            stream_count = len(self._stream_manager.list_streams())
            self._stream_manager.stop_all()
            logger.info(f"Stopped {stream_count} streams")
        except Exception as e:
            errors.append(f"Failed to stop streams: {e}")
            logger.error(f"Error stopping streams: {e}")

        # Damp the robot
        try:
            self._executor.execute("Damp")
            logger.info("Robot damped")
        except Exception as e:
            errors.append(f"Failed to damp robot: {e}")
            logger.error(f"Error damping robot: {e}")

        if errors:
            return {
                "status": "ok",
                "warning": "Partial success",
                "errors": errors
            }

        return {"status": "ok", "message": "All streams stopped, robot damped"}

    def _pub_loop(self):
        """Loop for publishing data to the PUB socket."""
        logger.info("Publisher thread started")

        while self._running:
            try:
                # Get data from queue with timeout
                try:
                    data = self._data_queue.get(timeout=0.1)
                    message = json.dumps(data)
                    self._pub_socket.send_string(message)
                except queue.Empty:
                    pass
            except Exception as e:
                if self._running:
                    logger.error(f"Error in publisher loop: {e}")

    def _heartbeat_loop(self):
        """Loop for sending heartbeat messages."""
        logger.info("Heartbeat thread started")
        interval = 1.0 / self.heartbeat_hz

        while self._running:
            try:
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": time.time(),
                    "uptime": time.time() - self._start_time,
                    "streams": self._stream_manager.list_streams() if self._stream_manager else [],
                    "subscriptions": self._subscription_manager.list_subscriptions() if self._subscription_manager else [],
                }
                self._pub_socket.send_string(json.dumps(heartbeat))
            except Exception as e:
                if self._running:
                    logger.error(f"Error in heartbeat loop: {e}")

            time.sleep(interval)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="ZMQ-to-DDS Bridge Server for Unitree G1"
    )
    parser.add_argument(
        "--rep-port",
        type=int,
        default=5556,
        help="Port for ZMQ REP socket (commands)"
    )
    parser.add_argument(
        "--pub-port",
        type=int,
        default=5555,
        help="Port for ZMQ PUB socket (topic data)"
    )
    parser.add_argument(
        "--heartbeat-hz",
        type=float,
        default=1.0,
        help="Heartbeat frequency in Hz"
    )
    parser.add_argument(
        "--interface",
        type=str,
        default=None,
        help="Network interface for DDS (e.g., eth0)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    server = BridgeServer(
        rep_port=args.rep_port,
        pub_port=args.pub_port,
        heartbeat_hz=args.heartbeat_hz
    )

    # Signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating graceful shutdown...")
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        server.start(network_interface=args.interface)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
