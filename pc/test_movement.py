#!/usr/bin/env python3
"""
Test movement commands on the G1 robot.

WARNING: This script will make the robot move!
Ensure the robot has clear space around it before running.

This script tests:
1. Boot sequence
2. Basic movement commands
3. Velocity control
4. Clean shutdown

Usage:
    python test_movement.py --ip 192.168.1.100
    python test_movement.py --ip 192.168.1.100 --no-move  # Dry run
"""

import argparse
import logging
import sys
import time

# Add parent directory to path for imports
sys.path.insert(0, '..')

from g1_robot import G1Robot
from g1_bridge_client import BridgeError


def test_boot(robot: G1Robot) -> bool:
    """Test robot boot sequence."""
    print("\n=== Test: Boot Sequence ===")
    try:
        robot.boot()
        print("  Boot successful")
        print(f"  Locomotion stream active: {robot.state.locomotion_stream_active}")
        return True
    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_velocity_control(robot: G1Robot, dry_run: bool = False) -> bool:
    """Test velocity control."""
    print("\n=== Test: Velocity Control ===")

    if dry_run:
        print("  [DRY RUN - no actual movement]")

    try:
        # Test forward
        print("  Setting forward velocity (0.2 m/s)...")
        if not dry_run:
            robot.set_velocity(0.2, 0, 0)
            time.sleep(2)
            robot.stop()
            time.sleep(0.5)

        # Test turning
        print("  Setting turn velocity (0.3 rad/s)...")
        if not dry_run:
            robot.set_velocity(0, 0, 0.3)
            time.sleep(2)
            robot.stop()
            time.sleep(0.5)

        # Test combined
        print("  Setting combined velocity (forward + turn)...")
        if not dry_run:
            robot.set_velocity(0.15, 0, 0.2)
            time.sleep(2)
            robot.stop()

        print("  Velocity control test complete")
        return True

    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_convenience_methods(robot: G1Robot, dry_run: bool = False) -> bool:
    """Test convenience movement methods."""
    print("\n=== Test: Convenience Methods ===")

    if dry_run:
        print("  [DRY RUN - no actual movement]")

    try:
        print("  Testing move_forward()...")
        if not dry_run:
            robot.move_forward(0.15)
            time.sleep(1)
            robot.stop()
            time.sleep(0.3)

        print("  Testing turn_left()...")
        if not dry_run:
            robot.turn_left(0.3)
            time.sleep(1)
            robot.stop()
            time.sleep(0.3)

        print("  Testing turn_right()...")
        if not dry_run:
            robot.turn_right(0.3)
            time.sleep(1)
            robot.stop()

        print("  Convenience methods test complete")
        return True

    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_posture_commands(robot: G1Robot, dry_run: bool = False) -> bool:
    """Test posture commands."""
    print("\n=== Test: Posture Commands ===")

    if dry_run:
        print("  [DRY RUN - no actual movement]")

    try:
        print("  Testing high_stand()...")
        if not dry_run:
            robot.high_stand()
            time.sleep(2)

        print("  Testing low_stand()...")
        if not dry_run:
            robot.low_stand()
            time.sleep(2)

        print("  Returning to normal stand...")
        if not dry_run:
            robot.high_stand()
            time.sleep(1)

        print("  Posture commands test complete")
        return True

    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_state_feedback(robot: G1Robot) -> bool:
    """Test state feedback from robot."""
    print("\n=== Test: State Feedback ===")

    try:
        print("  Waiting for state data (3 seconds)...")
        time.sleep(3)

        # Check IMU
        rpy = robot.get_imu_rpy()
        if rpy:
            print(f"  IMU RPY: roll={rpy['roll']:.3f}, pitch={rpy['pitch']:.3f}, yaw={rpy['yaw']:.3f}")
        else:
            print("  IMU: No data available")

        # Check odometry
        pos = robot.get_position()
        if pos:
            print(f"  Position: x={pos['x']:.3f}, y={pos['y']:.3f}, z={pos['z']:.3f}")
        else:
            print("  Position: No data available (odometry topic may not be publishing)")

        # Check velocity
        vel = robot.get_velocity()
        print(f"  Commanded velocity: vx={vel['vx']:.3f}, vy={vel['vy']:.3f}, vyaw={vel['vyaw']:.3f}")

        return True

    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_shutdown(robot: G1Robot) -> bool:
    """Test shutdown sequence."""
    print("\n=== Test: Shutdown ===")
    try:
        robot.shutdown()
        print("  Shutdown complete")
        return True
    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def run_interactive_mode(robot: G1Robot):
    """Run interactive control mode."""
    print("\n=== Interactive Mode ===")
    print("Commands:")
    print("  w/s - forward/backward")
    print("  a/d - turn left/right")
    print("  q/e - strafe left/right")
    print("  x   - stop")
    print("  h   - high stand")
    print("  l   - low stand")
    print("  ESC - exit")

    import sys
    import tty
    import termios

    old_settings = termios.tcgetattr(sys.stdin)

    try:
        tty.setcbreak(sys.stdin.fileno())

        speed = 0.3
        turn_speed = 0.5

        while True:
            ch = sys.stdin.read(1)

            if ch == '\x1b':  # ESC
                print("\nExiting interactive mode...")
                robot.stop()
                break
            elif ch == 'w':
                print("Forward")
                robot.set_velocity(speed, 0, 0)
            elif ch == 's':
                print("Backward")
                robot.set_velocity(-speed, 0, 0)
            elif ch == 'a':
                print("Turn left")
                robot.set_velocity(0, 0, turn_speed)
            elif ch == 'd':
                print("Turn right")
                robot.set_velocity(0, 0, -turn_speed)
            elif ch == 'q':
                print("Strafe left")
                robot.set_velocity(0, speed, 0)
            elif ch == 'e':
                print("Strafe right")
                robot.set_velocity(0, -speed, 0)
            elif ch == 'x':
                print("Stop")
                robot.stop()
            elif ch == 'h':
                print("High stand")
                robot.high_stand()
            elif ch == 'l':
                print("Low stand")
                robot.low_stand()

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)


def main():
    parser = argparse.ArgumentParser(
        description="Test G1 robot movement commands"
    )
    parser.add_argument(
        "--ip",
        type=str,
        required=True,
        help="Bridge server IP address"
    )
    parser.add_argument(
        "--rep-port",
        type=int,
        default=5556,
        help="REP socket port"
    )
    parser.add_argument(
        "--pub-port",
        type=int,
        default=5555,
        help="PUB socket port"
    )
    parser.add_argument(
        "--no-move",
        action="store_true",
        help="Dry run - don't actually move the robot"
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Run in interactive keyboard control mode"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    print("=" * 60)
    print("G1 Robot Movement Test")
    print("=" * 60)
    print(f"Bridge IP: {args.ip}")

    if args.no_move:
        print("\n*** DRY RUN MODE - Robot will NOT move ***\n")
    else:
        print("\n*** WARNING: Robot WILL move! ***")
        print("Ensure robot has clear space around it.")
        response = input("Continue? [y/N] ")
        if response.lower() != 'y':
            print("Aborted.")
            sys.exit(0)

    results = {}
    robot = None

    try:
        print("\nConnecting to robot...")
        robot = G1Robot(args.ip, args.rep_port, args.pub_port)
        robot.connect()
        print("Connected!")

        # Run tests
        results["boot"] = test_boot(robot)

        if results["boot"]:
            results["state_feedback"] = test_state_feedback(robot)

            if args.interactive:
                run_interactive_mode(robot)
            else:
                results["velocity_control"] = test_velocity_control(robot, args.no_move)
                results["convenience_methods"] = test_convenience_methods(robot, args.no_move)

                if not args.no_move:
                    results["posture_commands"] = test_posture_commands(robot, args.no_move)

            results["shutdown"] = test_shutdown(robot)

    except BridgeError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        if robot and robot.state.locomotion_stream_active:
            print("Stopping robot...")
            try:
                robot.stop()
                time.sleep(0.5)
                robot.damp()
            except:
                pass
    finally:
        if robot and robot.connected:
            print("\nDisconnecting...")
            robot.disconnect()

    # Summary
    if results:
        print("\n" + "=" * 60)
        print("Test Results Summary")
        print("=" * 60)

        passed = 0
        failed = 0
        for test_name, result in results.items():
            status = "PASS" if result else "FAIL"
            print(f"  {test_name}: {status}")
            if result:
                passed += 1
            else:
                failed += 1

        print("-" * 60)
        print(f"  Total: {passed} passed, {failed} failed")

        if failed > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
