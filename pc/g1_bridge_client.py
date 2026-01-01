#!/usr/bin/env python3
"""
Low-level ZMQ client for the G1 Bridge Server.

This client provides direct access to the bridge protocol:
- stream_create, stream_update, stream_stop
- subscribe, unsubscribe
- call (one-shot commands)
- ping

Usage:
    from g1_bridge_client import G1BridgeClient

    client = G1BridgeClient("192.168.1.100")
    client.connect()
    client.call("Start")
    client.stream_create("locomotion", "Move", {"vx": 0, "vy": 0, "vyaw": 0}, hz=50)
    client.stream_update("locomotion", {"vx": 0.5})
    client.disconnect()
"""

import json
import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import zmq

logger = logging.getLogger(__name__)


class BridgeError(Exception):
    """Exception raised for bridge communication errors."""

    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code


class G1BridgeClient:
    """
    Low-level client for the G1 ZMQ-to-DDS bridge.

    Provides:
    - Synchronous command interface (REQ-REP)
    - Asynchronous topic data reception (SUB)
    - Heartbeat monitoring
    - Thread-safe operation
    """

    def __init__(
        self,
        bridge_ip: str,
        rep_port: int = 5556,
        pub_port: int = 5555,
        timeout_ms: int = 5000
    ):
        """
        Initialize the bridge client.

        Args:
            bridge_ip: IP address of the bridge server
            rep_port: Port for REQ-REP socket (commands)
            pub_port: Port for SUB socket (topic data)
            timeout_ms: Timeout for commands in milliseconds
        """
        self.bridge_ip = bridge_ip
        self.rep_port = rep_port
        self.pub_port = pub_port
        self.timeout_ms = timeout_ms

        self._connected = False
        self._zmq_context: Optional[zmq.Context] = None
        self._req_socket: Optional[zmq.Socket] = None
        self._sub_socket: Optional[zmq.Socket] = None
        self._req_lock = threading.Lock()

        # Subscriber thread
        self._sub_thread: Optional[threading.Thread] = None
        self._sub_running = False

        # Callbacks for topic data and heartbeats
        self._topic_callbacks: Dict[str, List[Callable]] = {}
        self._heartbeat_callbacks: List[Callable] = []
        self._callbacks_lock = threading.Lock()

        # Last heartbeat time for connection monitoring
        self._last_heartbeat_time: float = 0
        self._last_heartbeat: Optional[Dict[str, Any]] = None

    @property
    def connected(self) -> bool:
        """Check if client is connected."""
        return self._connected

    @property
    def last_heartbeat_age(self) -> float:
        """Get age of last heartbeat in seconds."""
        if self._last_heartbeat_time == 0:
            return float('inf')
        return time.time() - self._last_heartbeat_time

    @property
    def last_heartbeat(self) -> Optional[Dict[str, Any]]:
        """Get last heartbeat data."""
        return self._last_heartbeat

    def connect(self) -> bool:
        """
        Connect to the bridge server.

        Returns:
            True if connection successful

        Raises:
            BridgeError: If connection fails
        """
        if self._connected:
            return True

        logger.info(f"Connecting to bridge at {self.bridge_ip}...")

        try:
            self._zmq_context = zmq.Context()

            # REQ socket for commands
            self._req_socket = self._zmq_context.socket(zmq.REQ)
            self._req_socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
            self._req_socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
            self._req_socket.setsockopt(zmq.LINGER, 0)
            self._req_socket.connect(f"tcp://{self.bridge_ip}:{self.rep_port}")

            # SUB socket for topic data
            self._sub_socket = self._zmq_context.socket(zmq.SUB)
            self._sub_socket.setsockopt(zmq.RCVTIMEO, 1000)
            self._sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            self._sub_socket.connect(f"tcp://{self.bridge_ip}:{self.pub_port}")

            # Start subscriber thread
            self._sub_running = True
            self._sub_thread = threading.Thread(target=self._sub_loop, daemon=True)
            self._sub_thread.start()

            # Verify connection with ping
            response = self.ping()
            if response.get("status") != "ok":
                raise BridgeError("Ping failed", code="PING_FAILED")

            self._connected = True
            logger.info(f"Connected to bridge (uptime: {response.get('uptime', 0):.1f}s)")
            return True

        except zmq.ZMQError as e:
            self._cleanup_sockets()
            raise BridgeError(f"Failed to connect: {e}", code="CONNECTION_FAILED")

    def disconnect(self):
        """Disconnect from the bridge server."""
        if not self._connected:
            return

        logger.info("Disconnecting from bridge...")

        self._sub_running = False
        if self._sub_thread:
            self._sub_thread.join(timeout=2.0)

        self._cleanup_sockets()
        self._connected = False

        logger.info("Disconnected from bridge")

    def _cleanup_sockets(self):
        """Clean up ZMQ sockets."""
        if self._req_socket:
            self._req_socket.close()
            self._req_socket = None

        if self._sub_socket:
            self._sub_socket.close()
            self._sub_socket = None

        if self._zmq_context:
            self._zmq_context.term()
            self._zmq_context = None

    def _send_command(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a command and wait for response.

        Args:
            cmd: Command dictionary

        Returns:
            Response dictionary

        Raises:
            BridgeError: If command fails
        """
        with self._req_lock:
            if not self._req_socket:
                raise BridgeError("Not connected", code="NOT_CONNECTED")

            try:
                self._req_socket.send_string(json.dumps(cmd))
                response_str = self._req_socket.recv_string()
                response = json.loads(response_str)

                if response.get("status") == "error":
                    raise BridgeError(
                        response.get("error", "Unknown error"),
                        code=response.get("code", "UNKNOWN")
                    )

                return response

            except zmq.Again:
                raise BridgeError("Command timed out", code="TIMEOUT")
            except zmq.ZMQError as e:
                raise BridgeError(f"ZMQ error: {e}", code="ZMQ_ERROR")
            except json.JSONDecodeError as e:
                raise BridgeError(f"Invalid response: {e}", code="INVALID_RESPONSE")

    def _sub_loop(self):
        """Subscriber thread loop for receiving topic data and heartbeats."""
        logger.debug("Subscriber thread started")

        while self._sub_running:
            try:
                if self._sub_socket.poll(timeout=100):
                    message = self._sub_socket.recv_string()
                    data = json.loads(message)
                    self._handle_pub_message(data)
            except zmq.Again:
                pass
            except zmq.ZMQError as e:
                if self._sub_running:
                    logger.error(f"Subscriber error: {e}")
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid message received: {e}")
            except Exception as e:
                if self._sub_running:
                    logger.error(f"Unexpected error in subscriber: {e}")

        logger.debug("Subscriber thread stopped")

    def _handle_pub_message(self, data: Dict[str, Any]):
        """Handle a message from the PUB socket."""
        msg_type = data.get("type")

        if msg_type == "heartbeat":
            self._last_heartbeat_time = time.time()
            self._last_heartbeat = data

            with self._callbacks_lock:
                for callback in self._heartbeat_callbacks:
                    try:
                        callback(data)
                    except Exception as e:
                        logger.error(f"Heartbeat callback error: {e}")

        elif msg_type == "topic_data":
            topic = data.get("topic")
            if topic:
                with self._callbacks_lock:
                    callbacks = self._topic_callbacks.get(topic, [])
                    for callback in callbacks:
                        try:
                            callback(data)
                        except Exception as e:
                            logger.error(f"Topic callback error: {e}")

    # =========================================================================
    # Public API
    # =========================================================================

    def ping(self) -> Dict[str, Any]:
        """
        Ping the bridge server.

        Returns:
            Response with status and uptime
        """
        return self._send_command({"cmd": "ping"})

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a one-shot command.

        Args:
            method: Method name (e.g., "Start", "Damp", "Move")
            params: Optional parameters for the method

        Returns:
            Response with status and result

        Examples:
            client.call("Start")
            client.call("Move", {"vx": 0.5, "vy": 0, "vyaw": 0})
            client.call("SetStandHeight", {"height": 0.5})
        """
        cmd = {"cmd": "call", "method": method}
        if params:
            cmd["params"] = params
        return self._send_command(cmd)

    def stream_create(
        self,
        stream_id: str,
        method: str,
        params: Dict[str, Any],
        hz: float = 50
    ) -> Dict[str, Any]:
        """
        Create a command stream.

        Args:
            stream_id: Unique identifier for the stream
            method: Method to execute (e.g., "Move")
            params: Initial parameters
            hz: Execution frequency

        Returns:
            Response with status

        Example:
            client.stream_create("locomotion", "Move", {"vx": 0, "vy": 0, "vyaw": 0}, hz=50)
        """
        return self._send_command({
            "cmd": "stream_create",
            "stream_id": stream_id,
            "method": method,
            "params": params,
            "hz": hz
        })

    def stream_update(self, stream_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update stream parameters.

        Args:
            stream_id: Stream identifier
            params: New parameters (merged with existing)

        Returns:
            Response with status

        Example:
            client.stream_update("locomotion", {"vx": 0.5, "vyaw": 0.1})
        """
        return self._send_command({
            "cmd": "stream_update",
            "stream_id": stream_id,
            "params": params
        })

    def stream_stop(self, stream_id: str) -> Dict[str, Any]:
        """
        Stop a command stream.

        Args:
            stream_id: Stream identifier

        Returns:
            Response with status
        """
        return self._send_command({
            "cmd": "stream_stop",
            "stream_id": stream_id
        })

    def subscribe(self, topic: str, hz: float = 10) -> Dict[str, Any]:
        """
        Subscribe to a DDS topic.

        Args:
            topic: Topic name (e.g., "rt/lowstate")
            hz: Forward rate (downsampling)

        Returns:
            Response with status

        Example:
            client.subscribe("rt/lowstate", hz=10)
        """
        return self._send_command({
            "cmd": "subscribe",
            "topic": topic,
            "hz": hz
        })

    def unsubscribe(self, topic: str) -> Dict[str, Any]:
        """
        Unsubscribe from a DDS topic.

        Args:
            topic: Topic name

        Returns:
            Response with status
        """
        return self._send_command({
            "cmd": "unsubscribe",
            "topic": topic
        })

    def list_topics(self) -> List[str]:
        """
        List all available topics.

        Returns:
            List of topic names
        """
        response = self._send_command({"cmd": "list_topics"})
        return response.get("topics", [])

    def list_methods(self) -> List[str]:
        """
        List all available methods.

        Returns:
            List of method names
        """
        response = self._send_command({"cmd": "list_methods"})
        return response.get("methods", [])

    def list_streams(self) -> List[Dict[str, Any]]:
        """
        List all active streams with statistics.

        Returns:
            List of stream info dictionaries
        """
        response = self._send_command({"cmd": "list_streams"})
        return response.get("streams", [])

    def list_subscriptions(self) -> List[Dict[str, Any]]:
        """
        List all active subscriptions with statistics.

        Returns:
            List of subscription info dictionaries
        """
        response = self._send_command({"cmd": "list_subscriptions"})
        return response.get("subscriptions", [])

    def status(self) -> Dict[str, Any]:
        """
        Get bridge status.

        Returns:
            Status dictionary with uptime, streams, subscriptions
        """
        return self._send_command({"cmd": "status"})

    def stop_all(self) -> Dict[str, Any]:
        """
        Emergency stop: stop all streams and damp the robot.

        This is a safety command that:
        1. Stops all active command streams
        2. Calls Damp() to put robot in safe mode

        Returns:
            Response with status and any warnings/errors
        """
        return self._send_command({"cmd": "stop_all"})

    # =========================================================================
    # Callback Registration
    # =========================================================================

    def on_topic(self, topic: str, callback: Callable[[Dict[str, Any]], None]):
        """
        Register a callback for topic data.

        Args:
            topic: Topic name
            callback: Function to call with topic data

        The callback receives a dictionary with:
            - type: "topic_data"
            - topic: Topic name
            - timestamp: Unix timestamp
            - data: Serialized message data
        """
        with self._callbacks_lock:
            if topic not in self._topic_callbacks:
                self._topic_callbacks[topic] = []
            self._topic_callbacks[topic].append(callback)

    def off_topic(self, topic: str, callback: Optional[Callable] = None):
        """
        Unregister a callback for topic data.

        Args:
            topic: Topic name
            callback: Specific callback to remove (or None to remove all)
        """
        with self._callbacks_lock:
            if topic in self._topic_callbacks:
                if callback:
                    self._topic_callbacks[topic] = [
                        cb for cb in self._topic_callbacks[topic] if cb != callback
                    ]
                else:
                    del self._topic_callbacks[topic]

    def on_heartbeat(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Register a callback for heartbeat messages.

        Args:
            callback: Function to call with heartbeat data

        The callback receives a dictionary with:
            - type: "heartbeat"
            - timestamp: Unix timestamp
            - uptime: Bridge uptime in seconds
            - streams: List of active stream IDs
            - subscriptions: List of active subscription topics
        """
        with self._callbacks_lock:
            self._heartbeat_callbacks.append(callback)

    def off_heartbeat(self, callback: Optional[Callable] = None):
        """
        Unregister a heartbeat callback.

        Args:
            callback: Specific callback to remove (or None to remove all)
        """
        with self._callbacks_lock:
            if callback:
                self._heartbeat_callbacks = [
                    cb for cb in self._heartbeat_callbacks if cb != callback
                ]
            else:
                self._heartbeat_callbacks.clear()

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


# =============================================================================
# Convenience function
# =============================================================================

def create_client(
    bridge_ip: str,
    rep_port: int = 5556,
    pub_port: int = 5555
) -> G1BridgeClient:
    """
    Create and connect a bridge client.

    Args:
        bridge_ip: IP address of the bridge server
        rep_port: Command port
        pub_port: Topic data port

    Returns:
        Connected G1BridgeClient instance
    """
    client = G1BridgeClient(bridge_ip, rep_port, pub_port)
    client.connect()
    return client
