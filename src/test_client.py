#!/usr/bin/env python3
"""
Test script for MTA GTFS-Realtime client
Fetches data from a sample feed and validates it
"""

import sys
import logging
from mta_client import MTAClient
from data_validator import DataValidator
from data_storage import DataStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_single_feed(feed_name: str = 'l'):
    """Test fetching and parsing a single feed"""
    logger.info(f"Testing {feed_name} feed...")

    # Initialize components
    client = MTAClient()
    validator = DataValidator()
    storage = DataStorage()

    # Test raw data fetching
    logger.info("Fetching raw protobuf data...")
    raw_data = client.fetch_feed_data(feed_name)

    if not raw_data:
        logger.error("Failed to fetch raw data")
        return False

    logger.info(f"Fetched {len(raw_data)} bytes of raw data")

    # Save raw data
    raw_file_path = storage.save_raw_feed_data(feed_name, raw_data)

    # Test standard GTFS parsing
    logger.info("Parsing with standard GTFS...")
    standard_parsed = client.parse_feed_data(raw_data)

    if standard_parsed:
        logger.info("Standard GTFS parsing successful")
        storage.save_parsed_feed_data(f"{feed_name}_standard", standard_parsed)

        # Validate standard data
        is_valid, errors, warnings = validator.validate_feed_data(standard_parsed)
        logger.info(f"Standard validation - Valid: {is_valid}")
        for error in errors:
            logger.error(f"Validation error: {error}")
        for warning in warnings:
            logger.warning(f"Validation warning: {warning}")

    # Test NYCT parsing
    logger.info("Parsing with NYCT extensions...")
    nyct_parsed = client.parse_feed_with_nyct(feed_name)

    if nyct_parsed:
        logger.info("NYCT parsing successful")
        storage.save_parsed_feed_data(f"{feed_name}_nyct", nyct_parsed)

        # Validate NYCT data
        is_valid, errors, warnings = validator.validate_feed_data(nyct_parsed)
        logger.info(f"NYCT validation - Valid: {is_valid}")
        for error in errors:
            logger.error(f"Validation error: {error}")
        for warning in warnings:
            logger.warning(f"Validation warning: {warning}")

        # Get statistics
        stats = validator.get_data_statistics(nyct_parsed)
        logger.info(f"Feed statistics: {stats}")

        # Save sample data
        if nyct_parsed.get('trips'):
            sample_trips = nyct_parsed['trips'][:3]  # First 3 trips
            storage.save_sample_data(
                feed_name,
                {'sample_trips': sample_trips},
                f"Sample trips from {feed_name} feed"
            )

        return True

    logger.error("Both parsing methods failed")
    return False

def test_multiple_feeds():
    """Test fetching multiple feeds"""
    logger.info("Testing multiple feeds...")

    client = MTAClient()
    feeds_to_test = ['l', 'nqrw', 'ace']

    # Test specific lines
    lines_data = client.get_lines_data(['L', 'N', 'Q'])
    logger.info(f"Got data for lines: {list(lines_data.keys())}")

    # Test all feeds
    all_feeds = client.get_all_feeds()
    logger.info(f"Fetched {len(all_feeds)} feeds")

    for feed_name, feed_data in all_feeds.items():
        validator = DataValidator()
        stats = validator.get_data_statistics(feed_data)
        logger.info(f"{feed_name}: {stats['total_trips']} trips, "
                   f"{stats['total_vehicles']} vehicles, "
                   f"routes: {stats['routes_covered']}")

def test_data_storage():
    """Test data storage functionality"""
    logger.info("Testing data storage...")

    storage = DataStorage()

    # List existing files
    files = storage.list_files()
    logger.info(f"Existing files: {files}")

    # Get summary
    summary = storage.create_data_summary()
    logger.info(f"Storage summary: {summary}")

    # Test loading latest files
    for feed in ['l', 'nqrw', 'ace']:
        latest_parsed = storage.get_latest_file(feed, 'parsed')
        if latest_parsed:
            logger.info(f"Latest {feed} parsed file: {latest_parsed}")
            data = storage.load_parsed_feed_data(latest_parsed)
            if data:
                logger.info(f"Loaded {feed} data successfully")

def main():
    """Main test function"""
    logger.info("Starting MTA GTFS-Realtime client tests...")

    try:
        # Test single feed first
        success = test_single_feed('l')  # L train is usually active
        if not success:
            logger.error("Single feed test failed")
            return False

        # Test data storage
        test_data_storage()

        # Test multiple feeds
        test_multiple_feeds()

        logger.info("All tests completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)