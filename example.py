#!/usr/bin/env python3
"""
Example usage of MTA GTFS-Realtime client
Demonstrates basic functionality and common use cases
"""

import sys
import os
import logging
from datetime import datetime

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from mta_client import MTAClient
from data_validator import DataValidator
from data_storage import DataStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def example_basic_usage():
    """Example 1: Basic feed fetching and parsing"""
    print("\n=== Example 1: Basic Usage ===")

    # Create client
    client = MTAClient()

    # Fetch L train data
    print("Fetching L train data...")
    raw_data = client.fetch_feed_data('l')

    if raw_data:
        print(f"✓ Fetched {len(raw_data)} bytes of raw data")

        # Parse the data
        parsed_data = client.parse_feed_data(raw_data)

        if parsed_data:
            print(f"✓ Parsed data successfully")

            # Count different entity types
            trip_updates = len([e for e in parsed_data['entities'] if 'trip_update' in e])
            vehicles = len([e for e in parsed_data['entities'] if 'vehicle' in e])
            alerts = len([e for e in parsed_data['entities'] if 'alert' in e])

            print(f"  - Trip updates: {trip_updates}")
            print(f"  - Vehicle positions: {vehicles}")
            print(f"  - Alerts: {alerts}")

            return parsed_data

    return None

def example_data_validation(parsed_data):
    """Example 2: Data validation"""
    print("\n=== Example 2: Data Validation ===")

    if not parsed_data:
        print("✗ No data to validate")
        return

    validator = DataValidator()

    # Validate the data
    is_valid, errors, warnings = validator.validate_feed_data(parsed_data)

    print(f"Data valid: {is_valid}")

    if errors:
        print("Errors found:")
        for error in errors:
            print(f"  ✗ {error}")

    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  ⚠ {warning}")

    # Get statistics
    stats = validator.get_data_statistics(parsed_data)
    print(f"\nFeed Statistics:")
    print(f"  - Routes covered: {stats['routes_covered']}")
    print(f"  - Data age: {stats['data_age_minutes']:.1f} minutes")
    print(f"  - Last update: {datetime.fromtimestamp(stats['last_update'])}")

def example_multiple_lines():
    """Example 3: Getting data for multiple lines"""
    print("\n=== Example 3: Multiple Lines ===")

    client = MTAClient()

    # Get data for specific lines
    lines = ['L', 'N', 'Q', '1', '4', '6']
    print(f"Fetching data for lines: {lines}")

    lines_data = client.get_lines_data(lines)

    validator = DataValidator()

    for feed_name, data in lines_data.items():
        stats = validator.get_data_statistics(data)
        print(f"  {feed_name}: {stats['routes_covered']} ({stats['total_trips']} trips)")

def example_data_storage():
    """Example 4: Data storage and retrieval"""
    print("\n=== Example 4: Data Storage ===")

    storage = DataStorage()

    # Show current storage status
    summary = storage.create_data_summary()
    print(f"Storage directory: {summary['directories']['parsed']}")
    print(f"Files stored: {summary['file_counts']}")
    print(f"Total size: {summary['total_size_mb']} MB")
    print(f"Feeds represented: {summary['feeds_represented']}")

    # List recent files
    files = storage.list_files()
    print(f"\nRecent files:")
    for data_type, file_list in files.items():
        print(f"  {data_type}: {len(file_list)} files")
        # Show latest 3 files
        for filename in sorted(file_list)[-3:]:
            print(f"    - {filename}")

    # Load latest parsed data
    for feed in ['l', 'nqrw']:
        latest_file = storage.get_latest_file(feed, 'parsed')
        if latest_file:
            print(f"\nLoading latest {feed} data...")
            data = storage.load_parsed_feed_data(latest_file)
            if data:
                entity_count = len(data.get('entities', []))
                print(f"  ✓ Loaded {entity_count} entities from {os.path.basename(latest_file)}")

def example_real_time_monitoring():
    """Example 5: Real-time monitoring simulation"""
    print("\n=== Example 5: Real-time Monitoring ===")

    client = MTAClient()
    validator = DataValidator()

    # Monitor specific lines
    lines_to_monitor = ['L', '4', '6']

    print(f"Monitoring lines: {lines_to_monitor}")
    print("(This would typically run in a loop)")

    # Simulate one monitoring cycle
    for line in lines_to_monitor:
        # Determine which feed contains this line
        feed_name = client.LINE_TO_FEED.get(line, 'unknown')

        if feed_name == 'unknown':
            print(f"  ✗ Unknown line: {line}")
            continue

        # Fetch data
        raw_data = client.fetch_feed_data(feed_name)
        if not raw_data:
            print(f"  ✗ Failed to fetch {line} line data")
            continue

        parsed_data = client.parse_feed_data(raw_data)
        if not parsed_data:
            print(f"  ✗ Failed to parse {line} line data")
            continue

        # Extract line-specific data
        line_trips = []
        line_vehicles = []

        for entity in parsed_data['entities']:
            # Check trip updates
            if 'trip_update' in entity:
                trip = entity['trip_update']
                if trip.get('route_id') == line:
                    line_trips.append(trip)

            # Check vehicle positions
            if 'vehicle' in entity:
                vehicle = entity['vehicle']
                if vehicle.get('route_id') == line:
                    line_vehicles.append(vehicle)

        print(f"  {line} line: {len(line_trips)} trips, {len(line_vehicles)} vehicles")

        # Show sample vehicle position
        if line_vehicles:
            vehicle = line_vehicles[0]
            pos = vehicle.get('position', {})
            lat, lon = pos.get('latitude'), pos.get('longitude')
            stop_id = vehicle.get('stop_id', 'unknown')

            if lat and lon:
                print(f"    Sample vehicle at: {lat:.4f}, {lon:.4f} (stop: {stop_id})")
            else:
                print(f"    Sample vehicle at stop: {stop_id}")

def example_error_handling():
    """Example 6: Error handling"""
    print("\n=== Example 6: Error Handling ===")

    client = MTAClient()

    # Test with invalid feed name
    print("Testing invalid feed name...")
    bad_data = client.fetch_feed_data('invalid_feed')
    print(f"Result: {bad_data}")  # Should be None

    # Test with very short timeout
    print("Testing short timeout...")
    slow_client = MTAClient(timeout=0.001)  # Very short timeout
    timeout_data = slow_client.fetch_feed_data('l')
    print(f"Result: {timeout_data}")  # Likely None due to timeout

def main():
    """Run all examples"""
    print("MTA GTFS-Realtime Client Examples")
    print("=" * 40)

    try:
        # Run examples in sequence
        parsed_data = example_basic_usage()
        example_data_validation(parsed_data)
        example_multiple_lines()
        example_data_storage()
        example_real_time_monitoring()
        example_error_handling()

        print("\n" + "=" * 40)
        print("✓ All examples completed successfully!")

    except Exception as e:
        print(f"\n✗ Example failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)