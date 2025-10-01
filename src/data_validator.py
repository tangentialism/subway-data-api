"""
Data validation module for MTA GTFS-Realtime feeds
Validates data quality and structure
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates MTA feed data for quality and completeness"""

    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []

    def validate_feed_data(self, feed_data: Dict) -> Tuple[bool, List[str], List[str]]:
        """
        Validate complete feed data

        Args:
            feed_data: Parsed feed data dictionary

        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        self.validation_errors = []
        self.validation_warnings = []

        # Check basic structure
        if not self._validate_basic_structure(feed_data):
            return False, self.validation_errors, self.validation_warnings

        # Check timestamp freshness
        self._validate_timestamp_freshness(feed_data)

        # Validate trips data
        if 'trips' in feed_data:
            self._validate_trips_data(feed_data['trips'])

        # Validate vehicles data
        if 'vehicles' in feed_data:
            self._validate_vehicles_data(feed_data['vehicles'])

        # Check for entities if using standard GTFS format
        if 'entities' in feed_data:
            self._validate_entities_data(feed_data['entities'])

        is_valid = len(self.validation_errors) == 0
        return is_valid, self.validation_errors, self.validation_warnings

    def _validate_basic_structure(self, feed_data: Dict) -> bool:
        """Validate basic feed structure"""
        if not isinstance(feed_data, dict):
            self.validation_errors.append("Feed data must be a dictionary")
            return False

        # Check for required fields
        if 'header' in feed_data:
            # Standard GTFS format
            header = feed_data['header']
            if 'timestamp' not in header:
                self.validation_errors.append("Missing timestamp in header")
                return False
        elif 'last_update' in feed_data:
            # NYCT format
            if not feed_data['last_update']:
                self.validation_errors.append("Missing last_update timestamp")
                return False
        else:
            self.validation_errors.append("Feed data missing timestamp information")
            return False

        return True

    def _validate_timestamp_freshness(self, feed_data: Dict):
        """Check if feed timestamp is recent"""
        current_time = datetime.now(timezone.utc).timestamp()
        feed_timestamp = None

        if 'header' in feed_data:
            feed_timestamp = feed_data['header'].get('timestamp')
        elif 'last_update' in feed_data:
            feed_timestamp = feed_data['last_update']

        if feed_timestamp:
            age_seconds = current_time - feed_timestamp
            age_minutes = age_seconds / 60

            if age_minutes > 10:
                self.validation_warnings.append(
                    f"Feed data is {age_minutes:.1f} minutes old (may be stale)"
                )
            elif age_minutes > 30:
                self.validation_errors.append(
                    f"Feed data is {age_minutes:.1f} minutes old (too stale)"
                )

    def _validate_trips_data(self, trips: List[Dict]):
        """Validate trips data structure"""
        if not isinstance(trips, list):
            self.validation_errors.append("Trips data must be a list")
            return

        for i, trip in enumerate(trips):
            if not isinstance(trip, dict):
                self.validation_errors.append(f"Trip {i} must be a dictionary")
                continue

            # Check required fields
            required_fields = ['trip_id', 'route_id']
            for field in required_fields:
                if field not in trip or not trip[field]:
                    self.validation_warnings.append(
                        f"Trip {i} missing required field: {field}"
                    )

            # Validate stop time updates
            if 'stop_time_updates' in trip:
                self._validate_stop_time_updates(trip['stop_time_updates'], i)

    def _validate_stop_time_updates(self, updates: List[Dict], trip_index: int):
        """Validate stop time updates"""
        if not isinstance(updates, list):
            self.validation_errors.append(
                f"Trip {trip_index} stop_time_updates must be a list"
            )
            return

        for j, update in enumerate(updates):
            if not isinstance(update, dict):
                self.validation_errors.append(
                    f"Trip {trip_index} stop update {j} must be a dictionary"
                )
                continue

            # Check for stop_id
            if 'stop_id' not in update or not update['stop_id']:
                self.validation_warnings.append(
                    f"Trip {trip_index} stop update {j} missing stop_id"
                )

            # Check for timing information
            has_arrival = 'arrival_time' in update and update['arrival_time']
            has_departure = 'departure_time' in update and update['departure_time']

            if not has_arrival and not has_departure:
                self.validation_warnings.append(
                    f"Trip {trip_index} stop update {j} missing timing information"
                )

    def _validate_vehicles_data(self, vehicles: List[Dict]):
        """Validate vehicles data structure"""
        if not isinstance(vehicles, list):
            self.validation_errors.append("Vehicles data must be a list")
            return

        for i, vehicle in enumerate(vehicles):
            if not isinstance(vehicle, dict):
                self.validation_errors.append(f"Vehicle {i} must be a dictionary")
                continue

            # Check for trip association
            if 'trip_id' not in vehicle or not vehicle['trip_id']:
                self.validation_warnings.append(
                    f"Vehicle {i} missing trip_id association"
                )

            # Check for position data
            if 'position' in vehicle:
                position = vehicle['position']
                if isinstance(position, dict):
                    lat = position.get('latitude')
                    lon = position.get('longitude')

                    if lat is None and lon is None:
                        self.validation_warnings.append(
                            f"Vehicle {i} missing position coordinates"
                        )
                    elif lat is not None and lon is not None:
                        # Validate NYC area coordinates
                        if not (40.4 <= lat <= 40.9 and -74.3 <= lon <= -73.7):
                            self.validation_warnings.append(
                                f"Vehicle {i} position outside NYC area: {lat}, {lon}"
                            )

    def _validate_entities_data(self, entities: List[Dict]):
        """Validate entities data (standard GTFS format)"""
        if not isinstance(entities, list):
            self.validation_errors.append("Entities must be a list")
            return

        trip_updates = 0
        vehicle_positions = 0
        alerts = 0

        for i, entity in enumerate(entities):
            if not isinstance(entity, dict):
                self.validation_errors.append(f"Entity {i} must be a dictionary")
                continue

            if 'id' not in entity:
                self.validation_errors.append(f"Entity {i} missing id field")

            # Count entity types
            if 'trip_update' in entity:
                trip_updates += 1
            if 'vehicle' in entity:
                vehicle_positions += 1
            if 'alert' in entity:
                alerts += 1

        # Log statistics
        logger.info(f"Feed contains: {trip_updates} trip updates, "
                   f"{vehicle_positions} vehicle positions, {alerts} alerts")

        if trip_updates == 0 and vehicle_positions == 0:
            self.validation_warnings.append("Feed contains no trip or vehicle data")

    def validate_line_coverage(self, feed_data: Dict, expected_lines: List[str]) -> Tuple[bool, List[str]]:
        """
        Validate that feed contains data for expected subway lines

        Args:
            feed_data: Parsed feed data
            expected_lines: List of expected subway lines

        Returns:
            Tuple of (has_coverage, missing_lines)
        """
        found_lines = set()

        # Extract route IDs from trips
        if 'trips' in feed_data:
            for trip in feed_data['trips']:
                if 'route_id' in trip and trip['route_id']:
                    found_lines.add(trip['route_id'])

        # Extract route IDs from entities
        if 'entities' in feed_data:
            for entity in feed_data['entities']:
                if 'trip_update' in entity:
                    route_id = entity['trip_update'].get('route_id')
                    if route_id:
                        found_lines.add(route_id)

                if 'vehicle' in entity:
                    route_id = entity['vehicle'].get('route_id')
                    if route_id:
                        found_lines.add(route_id)

        missing_lines = [line for line in expected_lines if line not in found_lines]
        has_coverage = len(missing_lines) == 0

        return has_coverage, missing_lines

    def get_data_statistics(self, feed_data: Dict) -> Dict:
        """
        Generate statistics about feed data

        Args:
            feed_data: Parsed feed data

        Returns:
            Dictionary with data statistics
        """
        stats = {
            'total_trips': 0,
            'total_vehicles': 0,
            'total_alerts': 0,
            'routes_covered': set(),
            'last_update': None,
            'data_age_minutes': None
        }

        # Get timestamp
        if 'header' in feed_data:
            stats['last_update'] = feed_data['header'].get('timestamp')
        elif 'last_update' in feed_data:
            stats['last_update'] = feed_data['last_update']

        # Calculate age
        if stats['last_update']:
            current_time = datetime.now(timezone.utc).timestamp()
            stats['data_age_minutes'] = (current_time - stats['last_update']) / 60

        # Count trips
        if 'trips' in feed_data:
            stats['total_trips'] = len(feed_data['trips'])
            for trip in feed_data['trips']:
                if 'route_id' in trip and trip['route_id']:
                    stats['routes_covered'].add(trip['route_id'])

        # Count vehicles
        if 'vehicles' in feed_data:
            stats['total_vehicles'] = len(feed_data['vehicles'])

        # Count entities
        if 'entities' in feed_data:
            for entity in feed_data['entities']:
                if 'trip_update' in entity:
                    stats['total_trips'] += 1
                    route_id = entity['trip_update'].get('route_id')
                    if route_id:
                        stats['routes_covered'].add(route_id)

                if 'vehicle' in entity:
                    stats['total_vehicles'] += 1

                if 'alert' in entity:
                    stats['total_alerts'] += 1

        # Convert set to list for JSON serialization
        stats['routes_covered'] = list(stats['routes_covered'])

        return stats