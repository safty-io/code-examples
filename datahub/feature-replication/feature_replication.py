#!/usr/bin/env python3
"""
Feature Replication Script

This script fetches feature data from an HTTP API using protobuf and saves it to SQLite database.
"""

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from feature_pb2 import UpdatedFeature

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class Feature:
    """Represents a feature to replicate"""

    entity: str
    feature: str
    data_type: str = "string"


class FeatureReplication:
    def __init__(self, config_file: str = "config.json"):
        """Initialize the feature replication with configuration"""
        self.config = self.load_config(config_file)
        self.db_path = "feature_data.db"
        self.init_database()

    def load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
                self.validate_config(config)
                return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file {config_file} not found")

    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate that the configuration has all required fields"""
        # Check API key
        api_key = config.get("api", {}).get("api_key", "")
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            raise ValueError("API key is missing or not configured")
        
        # Check base URL
        if not config.get("api", {}).get("base_url", ""):
            raise ValueError("API base URL is missing")
        
        # Check features
        if not config.get("features", []):
            raise ValueError("No features configured")
        
        # Validate protobuf support
        try:
            from feature_pb2 import UpdatedFeature
            logger.info("Configuration validated successfully - using protobuf for API communication")
        except ImportError as e:
            raise ValueError(f"Protobuf support not available: {e}")

    def init_database(self):
        """Initialize SQLite database with offsets table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create offsets table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS offsets (
                entity TEXT NOT NULL,
                feature TEXT NOT NULL,
                last_modified TEXT NOT NULL,
                PRIMARY KEY (entity, feature)
            )
        """)

        conn.commit()
        conn.close()

    def load_offsets(self) -> Dict[str, str]:
        """Load last processing offsets from SQLite database"""
        offsets = {}
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Check if the offsets table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='offsets'"
            )
            table_exists = cursor.fetchone()

            if not table_exists:
                logger.info("Offsets table does not exist yet, starting fresh")
                conn.close()
                return offsets

            cursor.execute("SELECT entity, feature, last_modified FROM offsets")
            rows = cursor.fetchall()

            for entity, feature, last_modified in rows:
                if entity and feature and last_modified:
                    feature_key = self.get_feature_key(entity, feature)
                    offsets[feature_key] = last_modified
                else:
                    logger.warning(
                        f"Skipping invalid offset record: entity='{entity}', feature='{feature}', last_modified='{last_modified}'"
                    )

            conn.close()
            logger.info(f"Successfully loaded {len(offsets)} offsets from database")

        except Exception as e:
            logger.error(f"Error loading offsets: {e}, starting fresh")

        return offsets

    def save_offsets(self, offsets: Dict[str, str]):
        """Save processing offsets to SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for feature_key, last_modified in offsets.items():
            # Parse the feature key back to entity and feature
            if "/" in feature_key:
                entity, feature = feature_key.split("/", 1)
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO offsets (entity, feature, last_modified)
                    VALUES (?, ?, ?)
                """,
                    (entity, feature, last_modified),
                )
            else:
                logger.warning(f"Invalid feature key format: {feature_key}")

        conn.commit()
        conn.close()

    def get_feature_key(self, entity: str, feature: str) -> str:
        """Generate a unique key for entity/feature combination"""
        return f"{entity}/{feature}"

    def fetch_feature_data(
        self, entity: str, feature: str, start_offset: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch feature data from the API using protobuf"""
        url = f"{self.config['api']['base_url']}/entities/{entity}/features/{feature}/history"

        headers = {
            "x-api-key": self.config["api"]["api_key"],
            "Accept": "application/x-protobuf",  # Request protobuf instead of JSON
        }

        params = {"start": start_offset, "limit": self.config.get("batch_size", 1000)}

        try:
            logger.info(
                f"Fetching data for {entity}/{feature} from offset {start_offset}"
            )
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=self.config.get("request_timeout", 30),
            )

            if response.status_code == 200:
                # Parse protobuf response
                raw_data = self.parse_protobuf_response(response.content)
                if raw_data:
                    return raw_data
                else:
                    logger.info(f"No more data available for {entity}/{feature}")
                    return []
            else:
                logger.warning(
                    f"HTTP {response.status_code} for {entity}/{feature}: {response.text}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {entity}/{feature}: {e}")
            return None

    def parse_protobuf_response(self, response_data: bytes) -> List[Dict[str, Any]]:
        """Parse length-delimited protobuf response and convert to dictionary format"""
        try:
            if not response_data:
                return []

            # Parse length-delimited protobuf messages
            parsed_features = []
            offset = 0

            while offset < len(response_data):
                # Read varint length prefix
                length, varint_size = self.read_varint(response_data, offset)
                if length == 0:
                    break

                offset += varint_size

                # Check if we have enough data for the message
                if offset + length > len(response_data):
                    logger.warning(
                        f"Incomplete message: expected {length} bytes, got {len(response_data) - offset}"
                    )
                    break

                # Extract the message data
                message_data = response_data[offset : offset + length]
                offset += length

                # Parse the UpdatedFeature message
                try:
                    updated_feature = UpdatedFeature()
                    updated_feature.ParseFromString(message_data)
                    parsed_features.append(updated_feature)
                except Exception as e:
                    logger.error(f"Failed to parse UpdatedFeature message: {e}")
                    continue

            # Convert protobuf messages to dictionary format compatible with existing code
            result = []
            for feature in parsed_features:
                feature_dict = {
                    "id": feature.entityId,
                    "_lastModified": self.convert_timestamp_to_iso(feature.timestamp),
                    "history": [],
                }

                # Process history values
                for value in feature.history:
                    value_dict = {
                        "timestamp": self.convert_timestamp_to_iso(value.timestamp),
                        "stringValue": list(value.stringValue),
                        "doubleValue": list(value.doubleValue),
                        "boolValue": list(value.boolValue),
                    }
                    feature_dict["history"].append(value_dict)

                result.append(feature_dict)

            return result

        except Exception as e:
            logger.error(
                f"Unexpected error parsing length-delimited protobuf response: {e}"
            )
            return []

    def read_varint(self, data: bytes, offset: int) -> tuple[int, int]:
        """Read a varint from the data starting at offset. Returns (value, bytes_consumed)"""
        value = 0
        shift = 0
        bytes_consumed = 0

        while offset + bytes_consumed < len(data):
            byte = data[offset + bytes_consumed]
            bytes_consumed += 1

            value |= (byte & 0x7F) << shift

            if (byte & 0x80) == 0:
                # MSB is 0, so this is the last byte
                break

            shift += 7

            if shift >= 64:
                raise ValueError("Varint is too long")

        return value, bytes_consumed

    def convert_timestamp_to_iso(self, timestamp: int) -> str:
        """Convert protobuf timestamp (milliseconds) to ISO format string"""
        try:
            if timestamp == 0:
                return datetime.now(timezone.utc).isoformat()

            # Convert milliseconds to seconds and create datetime
            timestamp_seconds = timestamp / 1000
            dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
            return dt.isoformat()

        except Exception as e:
            logger.warning(
                f"Failed to convert timestamp {timestamp}: {e}, using current time"
            )
            return datetime.now(timezone.utc).isoformat()

    def process_feature_data(
        self, entity: str, feature: str, data: List[Dict[str, Any]], data_type: str
    ) -> List[Dict[str, Any]]:
        """Process and transform feature data from protobuf format"""
        processed_events = []

        for event in data:
            entity_id = event.get("id", "")
            last_modified = event.get("_lastModified", "")

            # Process each value in the history
            for value_obj in event.get("history", []):
                value_timestamp = value_obj.get("timestamp", "")
                
                # Get values based on data type
                if data_type == "string":
                    values = value_obj.get("stringValue", [])
                elif data_type == "double":
                    values = value_obj.get("doubleValue", [])
                elif data_type == "boolean":
                    values = value_obj.get("boolValue", [])
                else:
                    values = value_obj.get("stringValue", [])

                # Create an event for each value
                for value in values:
                    processed_events.append({
                        "entity_id": entity_id,
                        "timestamp": last_modified,
                        "value_timestamp": value_timestamp,
                        "value": self.convert_value(value, data_type),
                        "data_type": data_type,
                        "processed_at": datetime.now(timezone.utc).isoformat(),
                    })

        return processed_events

    def convert_value(self, value: Any, data_type: str) -> Any:
        """Convert value to appropriate type"""
        if value is None:
            return None

        try:
            if data_type == "double":
                return float(value)
            elif data_type == "boolean":
                if isinstance(value, str):
                    return value.lower() in ["true", "1", "yes", "on"]
                return bool(value)
            else:  # string, date, or default
                return str(value)
        except (ValueError, TypeError):
            return str(value)

    def get_table_name(self, entity: str, feature: str) -> str:
        """Generate table name for entity/feature combination"""
        safe_entity = entity.replace("-", "_").replace(" ", "_")
        safe_feature = feature.replace("-", "_").replace(" ", "_")
        return f"{safe_entity}_{safe_feature}"

    def create_feature_table(self, entity: str, feature: str, data_type: str):
        """Create a table for the specific entity/feature combination with appropriate value column type"""
        table_name = self.get_table_name(entity, feature)
        
        # Map data types to SQLite column types
        column_type_map = {
            "string": "TEXT",
            "double": "REAL", 
            "boolean": "INTEGER",
            "date": "TEXT"
        }
        
        value_column_type = column_type_map.get(data_type, "TEXT")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                value_timestamp TEXT NOT NULL,
                value {value_column_type},
                data_type TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                UNIQUE(entity_id, value_timestamp)
            )
        """)

        # Create essential indexes only
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_entity_id ON {table_name}(entity_id)"
        )
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp)"
        )

        conn.commit()
        conn.close()

    def save_to_sqlite(self, entity: str, feature: str, events: List[Dict[str, Any]], data_type: str):
        """Save events to entity-specific SQLite table, removing previous records for the same entity_ids"""
        if not events:
            return

        # Create table if it doesn't exist
        self.create_feature_table(entity, feature, data_type)

        table_name = self.get_table_name(entity, feature)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get unique entity_ids from new events
        new_entity_ids = set(event.get("entity_id", "") for event in events)

        # Remove existing records for the same entity_ids
        if new_entity_ids:
            placeholders = ",".join(["?"] * len(new_entity_ids))
            cursor.execute(
                f"DELETE FROM {table_name} WHERE entity_id IN ({placeholders})",
                list(new_entity_ids),
            )

        # Insert new events
        for event in events:
            cursor.execute(
                f"""
                INSERT OR REPLACE INTO {table_name} 
                (entity_id, timestamp, value_timestamp, value, data_type, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    event.get("entity_id", ""),
                    event.get("timestamp", ""),
                    event.get("value_timestamp", ""),
                    event.get("value", ""),
                    event.get("data_type", ""),
                    event.get("processed_at", ""),
                ),
            )

        conn.commit()
        conn.close()

        logger.info(f"Saved {len(events)} events to table {table_name}")

    def get_latest_offset(self, events: List[Dict[str, Any]]) -> Optional[str]:
        """Get the latest _lastModified timestamp from events to use as next offset"""
        if not events:
            return None

        # Since the last message processed always contains the latest _lastModified,
        # we should use the timestamp from the last event in the array
        last_event = events[-1]
        latest_timestamp = last_event.get("_lastModified", "")

        if not latest_timestamp:
            logger.warning("Last event has no _lastModified timestamp")
            return None

        return latest_timestamp

    def replicate_feature(self, feature: Feature) -> bool:
        """Replicate a single feature"""
        logger.info(f"Starting replication for {feature.entity}/{feature.feature}")

        # Load current offsets
        offsets = self.load_offsets()
        feature_key = self.get_feature_key(feature.entity, feature.feature)

        # Get starting offset (default to epoch if not found)
        current_offset = offsets.get(feature_key, "1970-01-01T00:00:00Z")

        if feature_key in offsets:
            logger.info(
                f"Found existing offset for {feature.entity}/{feature.feature}: {current_offset}"
            )
        else:
            logger.info(
                f"No existing offset found for {feature.entity}/{feature.feature}, using default: {current_offset}"
            )

        total_processed = 0
        batch_count = 0

        # Continue fetching until no more data is available
        while True:
            batch_count += 1
            logger.info(
                f"Fetching batch {batch_count} for {feature.entity}/{feature.feature} from offset {current_offset}"
            )

            # Fetch data
            raw_data = self.fetch_feature_data(
                feature.entity, feature.feature, current_offset
            )

            if raw_data is None:
                logger.error(
                    f"Failed to fetch data for {feature.entity}/{feature.feature}"
                )
                return False

            if not raw_data:
                logger.info(
                    f"No more data available for {feature.entity}/{feature.feature}"
                )
                break

            # Process data
            processed_events = self.process_feature_data(
                feature.entity, feature.feature, raw_data, feature.data_type
            )

            # Save to SQLite
            self.save_to_sqlite(feature.entity, feature.feature, processed_events, feature.data_type)

            total_processed += len(processed_events)
            logger.info(
                f"Processed {len(processed_events)} events in batch {batch_count}"
            )

            # Get the latest offset from current batch for next iteration
            latest_offset = self.get_latest_offset(raw_data)
            if latest_offset:
                # Check if we're making progress (new offset is different from current)
                if latest_offset == current_offset:
                    logger.warning(
                        f"Offset hasn't changed from {current_offset}, stopping to avoid infinite loop"
                    )
                    break

                # Update the offset
                current_offset = latest_offset
                offsets[feature_key] = latest_offset
                self.save_offsets(offsets)
                logger.info(
                    f"Updated offset for {feature.entity}/{feature.feature} to {latest_offset}"
                )
            else:
                logger.warning(f"No offset found in batch {batch_count}, stopping replication")
                break

            # Small delay between batches to be respectful to the API
            time.sleep(0.5)

        logger.info(
            f"Completed replication for {feature.entity}/{feature.feature} - processed {total_processed} events in {batch_count} batches"
        )
        return True

    def run(self):
        """Run the feature replication for all configured features"""
        logger.info("Starting feature replication process using protobuf API")

        # Parse features from config
        features = []
        for feature_config in self.config.get("features", []):
            feature = Feature(
                entity=feature_config["entity"],
                feature=feature_config["feature"],
                data_type=feature_config.get("data_type", "string"),
            )
            features.append(feature)

        logger.info(f"Found {len(features)} features to replicate")

        # Process each feature
        success_count = 0
        for feature in features:
            try:
                if self.replicate_feature(feature):
                    success_count += 1
                else:
                    logger.error(f"Failed to replicate {feature.entity}/{feature.feature}")
            except Exception as e:
                logger.error(f"Error replicating {feature.entity}/{feature.feature}: {e}")

            # Small delay between features to be respectful to the API
            time.sleep(1)

        logger.info(
            f"Replication completed. Successfully processed {success_count}/{len(features)} features"
        )
        logger.info(f"Data saved to SQLite database: {self.db_path}")


def main():
    """Main entry point"""
    replication = FeatureReplication()
    replication.run()


if __name__ == "__main__":
    main()
