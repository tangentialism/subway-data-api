"""
MTA GTFS-Realtime API Client
Connects to MTA real-time feeds and parses subway data
"""

import requests
import logging
from typing import Dict, List, Optional
from google.transit import gtfs_realtime_pb2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MTAClient:
    """Client for connecting to MTA GTFS-Realtime feeds"""

    # MTA GTFS-Realtime feed URLs (no API key required as of 2025)
    FEED_URLS = {
        'ace': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace',
        'bdfm': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm',
        'g': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g',
        'jz': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz',
        'nqrw': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw',
        'l': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l',
        '123456': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs',
        'sir': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si'
    }

    # Line mappings to feeds
    LINE_TO_FEED = {
        'A': 'ace', 'C': 'ace', 'E': 'ace', 'H': 'ace', 'FS': 'ace',
        'B': 'bdfm', 'D': 'bdfm', 'F': 'bdfm', 'M': 'bdfm',
        'G': 'g',
        'J': 'jz', 'Z': 'jz',
        'N': 'nqrw', 'Q': 'nqrw', 'R': 'nqrw', 'W': 'nqrw',
        'L': 'l',
        '1': '123456', '2': '123456', '3': '123456',
        '4': '123456', '5': '123456', '6': '123456', '7': '123456', 'S': '123456',
        'SIR': 'sir'
    }

    def __init__(self, timeout: int = 30):
        """
        Initialize MTA client

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NYC-Subway-Tracker/1.0'
        })

    def fetch_feed_data(self, feed_name: str) -> Optional[bytes]:
        """
        Fetch raw protobuf data from MTA feed

        Args:
            feed_name: Name of feed (ace, bdfm, g, etc.)

        Returns:
            Raw protobuf data or None if error
        """
        if feed_name not in self.FEED_URLS:
            logger.error(f"Unknown feed name: {feed_name}")
            return None

        url = self.FEED_URLS[feed_name]

        try:
            logger.info(f"Fetching data from {feed_name} feed...")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            logger.info(f"Successfully fetched {len(response.content)} bytes from {feed_name}")
            return response.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {feed_name} feed: {e}")
            return None

    def parse_feed_data(self, feed_data: bytes) -> Optional[Dict]:
        """
        Parse protobuf data into structured format

        Args:
            feed_data: Raw protobuf data

        Returns:
            Parsed feed data or None if error
        """
        try:
            # Parse with standard GTFS-realtime
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(feed_data)

            result = {
                'header': {
                    'gtfs_realtime_version': feed_message.header.gtfs_realtime_version,
                    'timestamp': feed_message.header.timestamp,
                    'incrementality': feed_message.header.incrementality
                },
                'entities': []
            }

            for entity in feed_message.entity:
                entity_data = {'id': entity.id}

                if entity.HasField('trip_update'):
                    entity_data['trip_update'] = self._parse_trip_update(entity.trip_update)

                if entity.HasField('vehicle'):
                    entity_data['vehicle'] = self._parse_vehicle_position(entity.vehicle)

                if entity.HasField('alert'):
                    entity_data['alert'] = self._parse_alert(entity.alert)

                result['entities'].append(entity_data)

            return result

        except Exception as e:
            logger.error(f"Error parsing feed data: {e}")
            return None

    def parse_feed_with_nyct(self, feed_name: str) -> Optional[Dict]:
        """
        Parse feed using standard parsing (NYCT library has compatibility issues)

        Args:
            feed_name: Name of feed to parse

        Returns:
            Parsed feed data
        """
        try:
            # Fetch raw data and parse with standard method
            raw_data = self.fetch_feed_data(feed_name)
            if not raw_data:
                return None

            return self.parse_feed_data(raw_data)

        except Exception as e:
            logger.error(f"Error parsing {feed_name}: {e}")
            return None

    def _parse_trip_update(self, trip_update) -> Dict:
        """Parse trip update data"""
        return {
            'trip_id': trip_update.trip.trip_id if trip_update.trip.trip_id else None,
            'route_id': trip_update.trip.route_id if trip_update.trip.route_id else None,
            'start_time': trip_update.trip.start_time if trip_update.trip.start_time else None,
            'start_date': trip_update.trip.start_date if trip_update.trip.start_date else None,
            'schedule_relationship': trip_update.trip.schedule_relationship,
            'stop_time_updates': [
                {
                    'stop_sequence': stu.stop_sequence,
                    'stop_id': stu.stop_id,
                    'arrival_time': stu.arrival.time if stu.HasField('arrival') else None,
                    'departure_time': stu.departure.time if stu.HasField('departure') else None
                }
                for stu in trip_update.stop_time_update
            ]
        }

    def _parse_vehicle_position(self, vehicle) -> Dict:
        """Parse vehicle position data"""
        return {
            'trip_id': vehicle.trip.trip_id if vehicle.trip.trip_id else None,
            'route_id': vehicle.trip.route_id if vehicle.trip.route_id else None,
            'position': {
                'latitude': vehicle.position.latitude if vehicle.HasField('position') else None,
                'longitude': vehicle.position.longitude if vehicle.HasField('position') else None
            },
            'current_stop_sequence': vehicle.current_stop_sequence if vehicle.current_stop_sequence else None,
            'current_status': vehicle.current_status,
            'timestamp': vehicle.timestamp if vehicle.timestamp else None,
            'stop_id': vehicle.stop_id if vehicle.stop_id else None
        }

    def _parse_alert(self, alert) -> Dict:
        """Parse alert data"""
        return {
            'cause': alert.cause,
            'effect': alert.effect,
            'header_text': alert.header_text.translation[0].text if alert.header_text.translation else None,
            'description_text': alert.description_text.translation[0].text if alert.description_text.translation else None
        }

    def get_all_feeds(self) -> Dict[str, Dict]:
        """
        Fetch and parse all subway feeds

        Returns:
            Dictionary with feed names as keys and parsed data as values
        """
        all_feeds = {}

        for feed_name in self.FEED_URLS.keys():
            logger.info(f"Processing {feed_name} feed...")

            # Try NYCT parsing first for better data
            feed_data = self.parse_feed_with_nyct(feed_name)

            if feed_data:
                all_feeds[feed_name] = feed_data
            else:
                logger.warning(f"Failed to parse {feed_name} feed")

        return all_feeds

    def get_lines_data(self, lines: List[str]) -> Dict[str, Dict]:
        """
        Get data for specific subway lines

        Args:
            lines: List of subway line names (e.g., ['1', '2', '3'])

        Returns:
            Dictionary with feed data for requested lines
        """
        feeds_needed = set()

        # Determine which feeds we need
        for line in lines:
            if line in self.LINE_TO_FEED:
                feeds_needed.add(self.LINE_TO_FEED[line])
            else:
                logger.warning(f"Unknown subway line: {line}")

        # Fetch only needed feeds
        result = {}
        for feed_name in feeds_needed:
            feed_data = self.parse_feed_with_nyct(feed_name)
            if feed_data:
                result[feed_name] = feed_data

        return result