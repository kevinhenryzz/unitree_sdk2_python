#!/usr/bin/env python3
"""
Test connection to the G1 Bridge Server.

This script tests:
1. Basic connectivity (ping)
2. Listing available topics
3. Listing available methods
4. Subscribing to a topic and receiving data
5. Heartbeat monitoring

Usage:
    python test_connection.py --ip 192.168.1.100
    python test_connection.py --ip 192.168.1.100 --verbose
"""

import argparse
import logging
import sys
import time

# Add parent directory to path for imports
sys.path.insert(0, '..')

from g1_bridge_client import G1BridgeClient, BridgeError


def test_ping(client: G1BridgeClient) -> bool:
    """Test basic connectivity."""
    print("\n=== Test: Ping ===")
    try:
        response = client.ping()
        print(f"  Status: OK")
        print(f"  Bridge uptime: {response.get('uptime', 0):.1f}s")
        return True
    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_list_topics(client: G1BridgeClient) -> bool:
    """Test listing available topics."""
    print("\n=== Test: List Topics ===")
    try:
        topics = client.list_topics()
        print(f"  Found {len(topics)} topics:")
        for topic in topics:
            print(f"    - {topic}")
        return True
    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_list_methods(client: G1BridgeClient) -> bool:
    """Test listing available methods."""
    print("\n=== Test: List Methods ===")
    try:
        methods = client.list_methods()
        print(f"  Found {len(methods)} methods:")
        for method in methods:
            print(f"    - {method}")
        return True
    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_subscribe(client: G1BridgeClient, topic: str = "rt/lowstate") -> bool:
    """Test subscribing to a topic."""
    print(f"\n=== Test: Subscribe to {topic} ===")

    received_data = []

    def on_data(data):
        received_data.append(data)
        print(f"  Received data from {data.get('topic')} at {data.get('timestamp', 0):.3f}")

    try:
        # Register callback
        client.on_topic(topic, on_data)

        # Subscribe
        client.subscribe(topic, hz=5)
        print(f"  Subscribed to {topic} at 5Hz")

        # Wait for data
        print("  Waiting for data (5 seconds)...")
        time.sleep(5)

        # Unsubscribe
        client.unsubscribe(topic)
        client.off_topic(topic)

        if received_data:
            print(f"  Received {len(received_data)} messages")

            # Show sample of last message
            last_msg = received_data[-1]
            data = last_msg.get("data", {})

            if "imu_state" in data:
                imu = data["imu_state"]
                rpy = imu.get("rpy", [0, 0, 0])
                print(f"  Sample IMU RPY: roll={rpy[0]:.3f}, pitch={rpy[1]:.3f}, yaw={rpy[2]:.3f}")

            return True
        else:
            print("  WARNING: No data received (topic may not be publishing)")
            return True  # Not a failure, just no data

    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def test_heartbeat(client: G1BridgeClient) -> bool:
    """Test heartbeat monitoring."""
    print("\n=== Test: Heartbeat Monitoring ===")

    heartbeats = []

    def on_heartbeat(data):
        heartbeats.append(data)
        print(f"  Heartbeat: uptime={data.get('uptime', 0):.1f}s, "
              f"streams={len(data.get('streams', []))}, "
              f"subs={len(data.get('subscriptions', []))}")

    try:
        client.on_heartbeat(on_heartbeat)

        print("  Waiting for heartbeats (5 seconds)...")
        time.sleep(5)

        client.off_heartbeat()

        if heartbeats:
            print(f"  Received {len(heartbeats)} heartbeats")
            return True
        else:
            print("  WARNING: No heartbeats received")
            return False

    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def test_status(client: G1BridgeClient) -> bool:
    """Test status command."""
    print("\n=== Test: Status ===")
    try:
        status = client.status()
        print(f"  Uptime: {status.get('uptime', 0):.1f}s")
        print(f"  Active streams: {status.get('streams', [])}")
        print(f"  Active subscriptions: {status.get('subscriptions', [])}")
        return True
    except BridgeError as e:
        print(f"  FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test connection to G1 Bridge Server"
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
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="rt/lowstate",
        help="Topic to test subscription with"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    print("=" * 60)
    print("G1 Bridge Connection Test")
    print("=" * 60)
    print(f"Bridge IP: {args.ip}")
    print(f"REP Port: {args.rep_port}")
    print(f"PUB Port: {args.pub_port}")

    results = {}

    try:
        print("\nConnecting to bridge...")
        client = G1BridgeClient(args.ip, args.rep_port, args.pub_port)
        client.connect()
        print("Connected!")

        # Run tests
        results["ping"] = test_ping(client)
        results["list_topics"] = test_list_topics(client)
        results["list_methods"] = test_list_methods(client)
        results["status"] = test_status(client)
        results["heartbeat"] = test_heartbeat(client)
        results["subscribe"] = test_subscribe(client, args.topic)

    except BridgeError as e:
        print(f"\nConnection failed: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        if 'client' in locals():
            print("\nDisconnecting...")
            client.disconnect()

    # Summary
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
