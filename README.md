# Cluster Hash DuckDB Extension

This DuckDB extension adds two functions that calculate hash slots and node assignments for string values using the CRC16-XMODEM algorithm:

1. `hash_slot(key)` - Calculates the hash slot (0-16383) for a given string
2. `node(key, node_count)` - Returns which node would own the key in a cluster with `node_count` nodes

## Why is this useful?

- **Data Distribution Analysis**: Understand how your keys would be distributed across a cluster
- **Sharding Analysis**: Test key naming strategies for optimal data distribution
- **Cluster Planning**: Plan data distribution before implementing a clustered database
- **Troubleshooting**: Identify keys that might cause hot spots in your cluster

## How It Works

This extension implements a consistent hashing algorithm:

1. Extract the hash tag from the key (if any)
   - If the key contains `{...}`, use only the content inside the braces
   - Otherwise use the entire key
2. Calculate the CRC16-XMODEM hash of the key or hash tag
3. Take the hash modulo 16384 to get the slot number (0-16383)
4. For node assignment, divide slots evenly among nodes

## Building the Extension

### Prerequisites

- CMake (3.16 or higher)
- C++17 compatible compiler
- DuckDB development files

### Build Steps

```bash
# Clone this repository
git clone https://github.com/lebmatter/duckdb-cluster-hash.git
cd duckdb-cluster-hash

# Create build directory
mkdir build
cd build

# Configure and build
cmake ..
make

# Install (optional)
sudo make install
```

## Usage Examples

Load the extension:

```sql
LOAD 'cluster_hash';
```

Calculate hash slots for keys:

```sql
SELECT 
    'user:1000' AS key,
    hash_slot('user:1000') AS slot;
    
SELECT 
    '{user:profile}:1000' AS key,
    hash_slot('{user:profile}:1000') AS slot;
```

Analyze key distribution across nodes:

```sql
-- Create a table with sample keys
CREATE TABLE test_keys (key VARCHAR);
INSERT INTO test_keys VALUES
    ('user:1000'),
    ('product:5432'),
    ('{user:1000}:profile');
    
-- Analyze distribution
SELECT 
    key,
    hash_slot(key) AS slot,
    node(key, 3) AS node_id
FROM test_keys;
```

## Advanced Usage

### Understanding Hash Tags

This implementation uses hash tags (parts of a key enclosed in `{...}`) to force related keys onto the same node:

```sql
SELECT 
    '{user:1000}:profile' AS key1,
    '{user:1000}:sessions' AS key2,
    hash_slot('{user:1000}:profile') AS slot1,
    hash_slot('{user:1000}:sessions') AS slot2;
```

Both keys will have the same hash slot because they share the same hash tag.

### Optimizing Key Distribution

Check if your key naming strategy results in an even distribution:

```sql
-- Generate some test keys
CREATE TABLE test_keys AS 
    SELECT 'user:' || i AS key 
    FROM generate_series(1, 1000) t(i);
    
-- Check distribution across 3 nodes
SELECT 
    node(key, 3) AS node_id,
    COUNT(*) AS key_count
FROM test_keys
GROUP BY node_id
ORDER BY node_id;
```

## License

MIT License