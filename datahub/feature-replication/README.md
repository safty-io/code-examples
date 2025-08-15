# Feature Replication Script

Python implementation of feature replication job that fetches feature data from Predicti Datahub using protobuf and saves it to a SQLite database.

## Features

- Fetches feature data from the Predicti API using protobuf format
- Saves feature data to a SQLite database with separate tables per feature
- Tracks processing offsets to avoid reprocessing data
- Simple configuration via JSON file
- Automatic database schema creation and indexing
- **Protobuf support** for efficient data serialization
- Configuration validation including protobuf support

## What you'll learn:

- How to authenticate with Predicti's API
- Working with protobuf for efficient data exchange
- Managing feature data synchronization patterns
- Database design for feature storage and retrieval

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

This installs:

- `requests` library for HTTP API calls
- `protobuf` library for protobuf message handling

1. Configure the script by editing `config.json`:
   - Update the API key if needed
   - Add/remove features to replicate
   - Adjust batch size and timeout settings

## Usage

Run the script:

```bash
python feature_replication.py
```

The script will:

1. Load configuration from `config.json`
2. Validate configuration including protobuf support
3. Initialize a SQLite database (`feature_data.db`)
4. Process each configured feature using protobuf API
5. Save data to the database with proper indexing
6. Track processing offsets in the database

## Output

- **SQLite database**: Saved as `feature_data.db` with separate tables for each feature
- **Offset tracking**: Stored in the database to resume from last processed point
- **Logs**: Clean logging to console showing progress

## Database Schema

The SQLite database uses a table-per-feature approach:

### Feature Tables (one per entity-feature combination):

Each feature gets its own table named `{entity}_{feature}` (e.g., `dk_address_address_floor`):

- `id`: Primary key (auto-increment)
- `entity_id`: The entity identifier
- `lasst_modified`: Event timestamp
- `effective_from`: Value timestamp
- `value`: The actual feature value (column type varies by data type)
- `data_type`: The configured data type
- `processed_at`: When the record was processed

**Value column types by data type:**

- `string` → `TEXT`
- `double` → `REAL`
- `boolean` → `INTEGER` (SQLite stores booleans as integers)
- `date` → `TEXT`

### offsets table:

- `entity`: The entity type
- `feature`: The feature name
- `last_modified`: The last processed timestamp

## Configuration

The `config.json` file supports:

- `api.base_url`: API base URL
- `api.api_key`: API authentication key
- `features`: List of entity/feature combinations to replicate
- `batch_size`: Number of records to fetch per API call
- `request_timeout`: HTTP request timeout in seconds

## Protobuf Integration

The script uses protobuf for efficient data exchange:

- **API Communication**: Uses `Accept: application/x-protobuf` header
- **Data Format**: Receives `UpdatedFeature` messages containing:
  - `entityId`: Entity identifier
  - `lastModified`: Last modified timestamp (in milliseconds since the epoch)
  - `history`: Array of `Value` objects with:
    - `effectiveFrom`: Value timestamp (in milliseconds since the epoch)
    - `stringValue`: Array of string values, including date strings in ISO format (yyyy-mm-dd)
    - `doubleValue`: Array of double values
    - `boolValue`: Array of boolean values
    - `instantValue`: Array of timestamp values (as milliseconds since the epoch)

### Data Type Handling

The script extracts values based on the configured data type:

- `string` → Uses `stringValue` array
- `double` → Uses `doubleValue` array
- `boolean` → Uses `boolValue` array
- `datetime` → Uses `instantValue` array

### Generating Protobuf Files

If you need to generate the Python protobuf classes from the `feature.proto` schema, use:

```bash
protoc --python_out=. feature.proto
```

This command requires the Protocol Buffers compiler (`protoc`) to be installed on your system. It generates `feature_pb2.py` containing the Python classes used by the replication script.

**When to generate:**

- Setting up a project for the first time
- After modifying the protobuf schema
- When the generated file is missing

## Files

- `feature_replication.py`: Main replication script
- `config.json`: Configuration file
- `feature_pb2.py`: Generated protobuf classes
- `requirements.txt`: Python dependencies
- `feature_data.db`: SQLite database (created automatically)

## Notes

- Uses protobuf format for efficient data serialization
- Saves to SQLite database with separate tables per feature
- **Dynamic column typing**: Value columns use appropriate SQLite types based on feature data type
- Processes features sequentially
- Automatically creates database indexes for better query performance
- Uses SQLite's built-in UNIQUE constraints to prevent duplicate records
- Includes comprehensive error handling for protobuf parsing
- Validates configuration including protobuf support on startup
