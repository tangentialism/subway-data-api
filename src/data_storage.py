"""
Data storage module for MTA feed data
Handles saving and loading feed data for testing and development
"""

import json
import os
import pickle
from datetime import datetime, timezone
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class DataStorage:
    """Handles storage and retrieval of MTA feed data"""

    def __init__(self, data_dir: str = None):
        """
        Initialize data storage

        Args:
            data_dir: Directory to store data files (defaults to ../data)
        """
        if data_dir is None:
            # Default to data directory relative to this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        else:
            self.data_dir = data_dir

        # Create data directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)

        # Create subdirectories
        self.raw_dir = os.path.join(self.data_dir, 'raw')
        self.parsed_dir = os.path.join(self.data_dir, 'parsed')
        self.samples_dir = os.path.join(self.data_dir, 'samples')

        for directory in [self.raw_dir, self.parsed_dir, self.samples_dir]:
            os.makedirs(directory, exist_ok=True)

    def save_raw_feed_data(self, feed_name: str, raw_data: bytes) -> str:
        """
        Save raw protobuf feed data

        Args:
            feed_name: Name of the feed (ace, bdfm, etc.)
            raw_data: Raw protobuf bytes

        Returns:
            Path to saved file
        """
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f"{feed_name}_{timestamp}.pb"
        filepath = os.path.join(self.raw_dir, filename)

        with open(filepath, 'wb') as f:
            f.write(raw_data)

        logger.info(f"Saved raw feed data to {filepath}")
        return filepath

    def save_parsed_feed_data(self, feed_name: str, parsed_data: Dict) -> str:
        """
        Save parsed feed data as JSON

        Args:
            feed_name: Name of the feed
            parsed_data: Parsed data dictionary

        Returns:
            Path to saved file
        """
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f"{feed_name}_{timestamp}.json"
        filepath = os.path.join(self.parsed_dir, filename)

        # Convert any datetime objects to timestamps for JSON serialization
        serializable_data = self._make_json_serializable(parsed_data)

        with open(filepath, 'w') as f:
            json.dump(serializable_data, f, indent=2)

        logger.info(f"Saved parsed feed data to {filepath}")
        return filepath

    def save_sample_data(self, feed_name: str, sample_data: Dict,
                        description: str = None) -> str:
        """
        Save sample data for development and testing

        Args:
            feed_name: Name of the feed
            sample_data: Sample data to save
            description: Optional description of the sample

        Returns:
            Path to saved file
        """
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f"{feed_name}_sample_{timestamp}.json"
        filepath = os.path.join(self.samples_dir, filename)

        sample_with_metadata = {
            'description': description or f"Sample data for {feed_name}",
            'timestamp': timestamp,
            'feed_name': feed_name,
            'data': self._make_json_serializable(sample_data)
        }

        with open(filepath, 'w') as f:
            json.dump(sample_with_metadata, f, indent=2)

        logger.info(f"Saved sample data to {filepath}")
        return filepath

    def load_raw_feed_data(self, filepath: str) -> Optional[bytes]:
        """
        Load raw protobuf feed data

        Args:
            filepath: Path to raw data file

        Returns:
            Raw protobuf bytes or None if error
        """
        try:
            with open(filepath, 'rb') as f:
                data = f.read()
            logger.info(f"Loaded raw feed data from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading raw feed data from {filepath}: {e}")
            return None

    def load_parsed_feed_data(self, filepath: str) -> Optional[Dict]:
        """
        Load parsed feed data from JSON

        Args:
            filepath: Path to parsed data file

        Returns:
            Parsed data dictionary or None if error
        """
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded parsed feed data from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading parsed feed data from {filepath}: {e}")
            return None

    def load_sample_data(self, filepath: str) -> Optional[Dict]:
        """
        Load sample data

        Args:
            filepath: Path to sample data file

        Returns:
            Sample data or None if error
        """
        try:
            with open(filepath, 'r') as f:
                sample = json.load(f)
            logger.info(f"Loaded sample data from {filepath}")
            return sample
        except Exception as e:
            logger.error(f"Error loading sample data from {filepath}: {e}")
            return None

    def list_files(self, data_type: str = 'all') -> Dict[str, List[str]]:
        """
        List stored data files

        Args:
            data_type: Type of data to list ('raw', 'parsed', 'samples', 'all')

        Returns:
            Dictionary with file listings by type
        """
        listings = {}

        if data_type in ['raw', 'all']:
            listings['raw'] = [
                f for f in os.listdir(self.raw_dir)
                if f.endswith('.pb')
            ]

        if data_type in ['parsed', 'all']:
            listings['parsed'] = [
                f for f in os.listdir(self.parsed_dir)
                if f.endswith('.json')
            ]

        if data_type in ['samples', 'all']:
            listings['samples'] = [
                f for f in os.listdir(self.samples_dir)
                if f.endswith('.json')
            ]

        return listings

    def get_latest_file(self, feed_name: str, data_type: str) -> Optional[str]:
        """
        Get the most recent file for a feed

        Args:
            feed_name: Name of the feed
            data_type: Type of data ('raw', 'parsed', 'samples')

        Returns:
            Path to most recent file or None if not found
        """
        directory_map = {
            'raw': self.raw_dir,
            'parsed': self.parsed_dir,
            'samples': self.samples_dir
        }

        if data_type not in directory_map:
            logger.error(f"Unknown data type: {data_type}")
            return None

        directory = directory_map[data_type]
        extension = '.pb' if data_type == 'raw' else '.json'

        # Find files matching the feed name
        matching_files = [
            f for f in os.listdir(directory)
            if f.startswith(feed_name) and f.endswith(extension)
        ]

        if not matching_files:
            return None

        # Sort by filename (includes timestamp) and get the latest
        latest_file = sorted(matching_files)[-1]
        return os.path.join(directory, latest_file)

    def cleanup_old_files(self, keep_days: int = 7):
        """
        Remove files older than specified days

        Args:
            keep_days: Number of days to keep files
        """
        from datetime import timedelta

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=keep_days)
        cutoff_timestamp = cutoff_time.timestamp()

        for directory in [self.raw_dir, self.parsed_dir, self.samples_dir]:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)

                try:
                    file_mtime = os.path.getmtime(filepath)
                    if file_mtime < cutoff_timestamp:
                        os.remove(filepath)
                        logger.info(f"Removed old file: {filepath}")
                except Exception as e:
                    logger.warning(f"Error removing file {filepath}: {e}")

    def _make_json_serializable(self, data):
        """Convert data to JSON-serializable format"""
        if isinstance(data, dict):
            return {key: self._make_json_serializable(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._make_json_serializable(item) for item in data]
        elif isinstance(data, datetime):
            return data.timestamp()
        elif hasattr(data, '__dict__'):
            # Handle objects with attributes
            return self._make_json_serializable(data.__dict__)
        else:
            return data

    def create_data_summary(self) -> Dict:
        """
        Create a summary of stored data

        Returns:
            Dictionary with data summary statistics
        """
        summary = {
            'directories': {
                'raw': self.raw_dir,
                'parsed': self.parsed_dir,
                'samples': self.samples_dir
            },
            'file_counts': {},
            'total_size_mb': 0,
            'feeds_represented': set()
        }

        # Count files and calculate sizes
        for data_type, directory in summary['directories'].items():
            files = os.listdir(directory)
            summary['file_counts'][data_type] = len(files)

            # Calculate total size
            for filename in files:
                filepath = os.path.join(directory, filename)
                try:
                    size = os.path.getsize(filepath)
                    summary['total_size_mb'] += size / (1024 * 1024)

                    # Extract feed name from filename
                    feed_name = filename.split('_')[0]
                    summary['feeds_represented'].add(feed_name)
                except Exception as e:
                    logger.warning(f"Error getting size for {filepath}: {e}")

        # Convert set to list for JSON serialization
        summary['feeds_represented'] = list(summary['feeds_represented'])
        summary['total_size_mb'] = round(summary['total_size_mb'], 2)

        return summary