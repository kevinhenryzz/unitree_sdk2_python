# ZMQ-to-DDS Bridge for Unitree G1

A bridge that runs on the Jetson inside the G1 robot, allowing a PC to control the robot over WiFi without needing DDS configured on the PC.

## Architecture

```
PC (WiFi) ←──── ZMQ ────→ Jetson Bridge ←──── DDS ────→ RockChip/Robot
                              │
                         Runs locally:
                         - Command streams at configured Hz
                         - Topic subscriptions with downsampling
```

## Core Design Principle

The bridge is a **dumb pipe with timers**. All robot control logic lives on the PC client. The bridge only:

1. Executes commands it's told to execute
2. Runs command streams at specified frequencies
3. Forwards subscribed DDS topics to the PC
4. Never crashes - always returns errors to client

## Key Concept: Streams

Instead of PC sending `Move()` 50 times per second over WiFi, PC does:

1. Create stream: "send `Move(vx, vy, vyaw)` at 50Hz"
2. Update stream parameters when velocity changes (rare)
3. Bridge runs the 50Hz loop locally

This keeps WiFi traffic low while maintaining high-frequency robot control.

## Installation

### On Jetson (inside robot)

```bash
# The bridge requires the unitree_sdk2_python to be installed
cd ~/unitree_sdk2_python
pip3 install -e .

# Install ZMQ
pip3 install pyzmq
```

### On PC

```bash
# Only need ZMQ on PC side
pip3 install pyzmq
```

## Quick Start

### 1. Start the Bridge (on Jetson)

SSH into the Jetson and run:

```bash
cd ~/unitree_sdk2_python
python3 jetson/bridge_server.py --interface eth0
```

Options:
- `--rep-port 5556`: Port for commands (default: 5556)
- `--pub-port 5555`: Port for topic data (default: 5555)
- `--interface eth0`: Network interface for DDS
- `--heartbeat-hz 1`: Heartbeat frequency (default: 1Hz)
- `--debug`: Enable debug logging

### 2. Test Connection (on PC)

```bash
cd ~/unitree_sdk2_python/pc
python3 test_connection.py --ip <JETSON_IP>
```

### 3. Test Movement (on PC)

```bash
cd ~/unitree_sdk2_python/pc
python3 test_movement.py --ip <JETSON_IP>

# Dry run (no actual movement):
python3 test_movement.py --ip <JETSON_IP> --no-move

# Interactive keyboard control:
python3 test_movement.py --ip <JETSON_IP> --interactive
```

## Usage Examples

### High-Level Interface (Recommended)

```python
from pc.g1_robot import G1Robot

# Connect to robot
robot = G1Robot("192.168.1.100")
robot.connect()

# Boot the robot (starts locomotion mode + creates movement stream)
robot.boot()

# Move the robot
robot.set_velocity(0.3, 0, 0)    # Walk forward at 0.3 m/s
time.sleep(3)
robot.set_velocity(0, 0, 0.5)   # Turn left at 0.5 rad/s
time.sleep(2)
robot.stop()                     # Stop all movement

# Posture commands
robot.high_stand()
robot.low_stand()
robot.wave_hand()

# Get sensor data
rpy = robot.get_imu_rpy()
print(f"Roll: {rpy['roll']}, Pitch: {rpy['pitch']}, Yaw: {rpy['yaw']}")

# Shutdown safely
robot.shutdown()
robot.disconnect()
```

### Low-Level Interface

```python
from pc.g1_bridge_client import G1BridgeClient

client = G1BridgeClient("192.168.1.100")
client.connect()

# One-shot commands
client.call("Start")
client.call("Move", {"vx": 0.3, "vy": 0, "vyaw": 0})

# Create a stream for continuous movement
client.stream_create("locomotion", "Move",
                     {"vx": 0, "vy": 0, "vyaw": 0}, hz=50)

# Update stream parameters (only sends when values change)
client.stream_update("locomotion", {"vx": 0.5})

# Subscribe to topics (must register first!)
def on_lowstate(data):
    imu = data["data"]["imu_state"]
    print(f"IMU: {imu['rpy']}")

# Register topic with message type, then subscribe
client.register_topic("rt/lowstate", "LowState")
client.on_topic("rt/lowstate", on_lowstate)
client.subscribe("rt/lowstate", hz=10)

# Cleanup
client.stream_stop("locomotion")
client.disconnect()
```

## Topic Registration

**Important:** Topics are NOT pre-registered. You must register topics before subscribing.

This design is intentional because:
- DDS topics are hardware/mode dependent (LiDAR may be off, hands not equipped, etc.)
- Pre-registering creates false expectations
- You know your G1 configuration better than the bridge does

### Registration Flow

```python
# 1. List known G1 topics (for reference)
known = client.list_known_topics()
# Returns: {"rt/lowstate": {"type": "LowState", "description": "..."}, ...}

# 2. List available message types
types = client.list_msg_types()
# Returns: ["LowState", "LowCmd", "Odometry", ...]

# 3. Register topics you want to use
client.register_topic("rt/lowstate", "LowState")
client.register_topic("rt/bms_state", "BmsState")

# 4. Now you can subscribe
client.subscribe("rt/lowstate", hz=10)
```

## Known G1 Topics

These topics are documented for reference. Register only the ones you need.

| Topic | Type | Description |
|-------|------|-------------|
| `rt/lowstate` | `LowState` | Robot state: joints, IMU, motors (always available) |
| `rt/lf/lowstate` | `LowState` | Low state via wireless controller path |
| `rt/arm_sdk` | `LowCmd` | Arm control commands (if arms enabled) |
| `rt/lowcmd` | `LowCmd` | Low-level motor commands |
| `rt/bms_state` | `BmsState` | Battery management system state |
| `rt/left_hand/state` | `HandState` | Left Dex3 hand state (if equipped) |
| `rt/right_hand/state` | `HandState` | Right Dex3 hand state (if equipped) |
| `rt/wirelesscontroller` | `WirelessController` | Wireless controller input (if connected) |
| `rt/utlidar/robot_odom` | `Odometry` | Robot odometry from LiDAR (if enabled) |
| `rt/odom` | `Odometry` | Robot odometry |
| `rt/sportmodestate` | `SportModeState` | Sport mode locomotion state |
| `rt/utlidar/cloud` | `PointCloud2` | LiDAR point cloud (if enabled) |
| `rt/utlidar/switch` | `String` | LiDAR on/off control |

## Available Message Types

| Type | Description |
|------|-------------|
| `LowState` | G1 joint states, IMU, motor feedback |
| `LowCmd` | G1 motor commands |
| `BmsState` | Battery management state |
| `HandState` | Dex3 hand state |
| `SportModeState` | Locomotion mode state |
| `WirelessController` | Controller joystick/button input |
| `Odometry` | Position/velocity odometry |
| `PointCloud2` | LiDAR point cloud |
| `String` | Simple string message |

## Available Commands

### One-Shot Commands (via `call`)

| Method | Parameters | Description |
|--------|------------|-------------|
| `Damp` | None | Passive damping mode |
| `Start` | None | Enter locomotion mode |
| `StandUp` | None | Stand up from lying |
| `Lie2StandUp` | None | Stand up from lying |
| `Squat2StandUp` | None | Stand from squat |
| `StandUp2Squat` | None | Squat from standing |
| `Sit` | None | Sit down |
| `ZeroTorque` | None | Zero torque (emergency) |
| `StopMove` | None | Stop movement |
| `HighStand` | None | High standing position |
| `LowStand` | None | Low standing position |
| `Move` | `{vx, vy, vyaw, continuous}` | Set velocity |
| `SetVelocity` | `{vx, vy, omega, duration}` | Set velocity with duration |
| `SetStandHeight` | `{height}` | Set standing height |
| `SetBalanceMode` | `{mode}` | Set balance mode |
| `BalanceStand` | `{mode}` | Balance stand mode |
| `WaveHand` | `{turn_flag}` | Wave hand gesture |
| `ShakeHand` | `{stage}` | Shake hand gesture |
| `SetFsmId` | `{fsm_id}` | Set FSM state |
| `SetTaskId` | `{task_id}` | Set arm task ID |

### Streamable Commands

Any command can be streamed at a specified frequency:

```python
# Create a Move stream at 50Hz
client.stream_create("locomotion", "Move",
                     {"vx": 0, "vy": 0, "vyaw": 0}, hz=50)

# Update parameters
client.stream_update("locomotion", {"vx": 0.5})

# Stop the stream
client.stream_stop("locomotion")
```

## Message Protocol

### PC → Bridge (REQ-REP)

```json
// Topic registration (required before subscribing)
{"cmd": "register_topic", "topic": "rt/lowstate", "msg_type": "LowState"}
{"cmd": "unregister_topic", "topic": "rt/lowstate"}

// Topic subscriptions (topic must be registered first)
{"cmd": "subscribe", "topic": "rt/lowstate", "hz": 10}
{"cmd": "unsubscribe", "topic": "rt/lowstate"}

// Stream management
{"cmd": "stream_create", "stream_id": "locomotion", "method": "Move",
 "params": {"vx": 0, "vy": 0, "vyaw": 0}, "hz": 50}
{"cmd": "stream_update", "stream_id": "locomotion", "params": {"vx": 0.5}}
{"cmd": "stream_stop", "stream_id": "locomotion"}

// One-shot commands
{"cmd": "call", "method": "Start"}
{"cmd": "call", "method": "Move", "params": {"vx": 0.5, "vy": 0, "vyaw": 0}}

// Emergency stop
{"cmd": "stop_all"}

// Discovery
{"cmd": "list_known_topics"}  // Reference list of G1 topics
{"cmd": "list_msg_types"}     // Available message types
{"cmd": "list_topics"}        // Currently registered topics
{"cmd": "list_methods"}       // Available robot commands

// Status
{"cmd": "ping"}
{"cmd": "status"}
{"cmd": "list_streams"}
{"cmd": "list_subscriptions"}
```

### Bridge → PC Responses

```json
// Success
{"status": "ok", "result": <data>}

// Error
{"status": "error", "error": "description", "code": "ERROR_CODE"}
```

### Bridge → PC (PUB)

```json
// Topic data
{"type": "topic_data", "topic": "rt/lowstate",
 "timestamp": 1234567890.123, "data": {...}}

// Heartbeat (every 1 second)
{"type": "heartbeat", "timestamp": 1234567890.123,
 "uptime": 3600.5, "streams": ["locomotion"],
 "subscriptions": ["rt/lowstate"]}
```

## G1 Joint Indices

```python
from pc.g1_robot import G1JointIndex

# Left leg (0-5)
G1JointIndex.LeftHipPitch    # 0
G1JointIndex.LeftHipRoll     # 1
G1JointIndex.LeftHipYaw      # 2
G1JointIndex.LeftKnee        # 3
G1JointIndex.LeftAnklePitch  # 4
G1JointIndex.LeftAnkleRoll   # 5

# Right leg (6-11)
G1JointIndex.RightHipPitch   # 6
# ... etc

# Waist (12-14)
G1JointIndex.WaistYaw        # 12
G1JointIndex.WaistRoll       # 13
G1JointIndex.WaistPitch      # 14

# Left arm (15-21)
G1JointIndex.LeftShoulderPitch  # 15
# ... etc

# Right arm (22-28)
G1JointIndex.RightShoulderPitch # 22
# ... etc
```

## Error Handling

The bridge never crashes. All errors are returned to the client:

```python
from pc.g1_bridge_client import BridgeError

try:
    client.call("UnknownMethod")
except BridgeError as e:
    print(f"Error: {e}")
    print(f"Code: {e.code}")  # e.g., "UNKNOWN_METHOD"
```

Error codes:
- `UNKNOWN_COMMAND`: Command not recognized
- `UNKNOWN_METHOD`: LocoClient method not found
- `STREAM_NOT_FOUND`: Stream ID doesn't exist
- `STREAM_EXISTS`: Stream ID already exists
- `TOPIC_NOT_FOUND`: Topic not registered
- `INVALID_PARAMS`: Invalid parameters
- `DDS_ERROR`: DDS communication failure
- `TIMEOUT`: Command timed out
- `NOT_CONNECTED`: Client not connected

## Heartbeat Monitoring

The bridge sends heartbeats every second. Monitor them on the PC:

```python
def on_heartbeat(data):
    age = time.time() - data["timestamp"]
    if age > 2.0:
        print("WARNING: Bridge may be disconnected!")

client.on_heartbeat(on_heartbeat)
```

## Troubleshooting

### Bridge won't start

1. Check that unitree_sdk2_python is installed: `pip3 list | grep unitree`
2. Check DDS interface: `--interface` should match your network (e.g., `eth0`, `wlan0`)
3. Check for port conflicts: `netstat -tlnp | grep 555`

### Can't connect from PC

1. Check network connectivity: `ping <JETSON_IP>`
2. Check firewall: `sudo ufw status`
3. Check bridge is running: `ps aux | grep bridge_server`

### No topic data received

1. Check robot is publishing: Use `ros2 topic list` on Jetson
2. Check subscription: `client.list_subscriptions()`
3. Check DDS domain ID matches (default: 0)

### Robot not moving

1. Check robot is in correct state: Call `Start` first
2. Check locomotion stream is active: `robot.state.locomotion_stream_active`
3. Check for errors in bridge logs
4. Ensure robot has cleared its startup sequence

## Files

```
unitree_sdk2_python/
├── jetson/
│   ├── __init__.py
│   └── bridge_server.py      # Bridge server (runs on Jetson)
├── pc/
│   ├── __init__.py
│   ├── g1_bridge_client.py   # Low-level ZMQ client
│   ├── g1_robot.py           # High-level robot interface
│   ├── test_connection.py    # Connection test script
│   └── test_movement.py      # Movement test script
└── BRIDGE_README.md          # This file
```
