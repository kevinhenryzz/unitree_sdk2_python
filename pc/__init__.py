"""
PC-side components for controlling the G1 robot via the ZMQ bridge.

This module contains:
- G1BridgeClient: Low-level ZMQ client for bridge protocol
- G1Robot: High-level robot interface
- BridgeError: Exception class for bridge errors
"""

from .g1_bridge_client import G1BridgeClient, BridgeError, create_client
from .g1_robot import G1Robot, G1JointIndex, RobotState, create_robot

__all__ = [
    'G1BridgeClient',
    'G1Robot',
    'G1JointIndex',
    'RobotState',
    'BridgeError',
    'create_client',
    'create_robot',
]
