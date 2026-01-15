# JanusGraph Cassandra Schema - Actual Retrieved Schema

**Generated On:** 2026-01-14
**Keyspace:** janusgraph
**Cassandra Version:** 3.11

---

## How to Retrieve This Schema

### Connect to Cassandra
```bash
# From host machine
docker exec -it cassandra cqlsh

# From inside container (or remote)
cqlsh <cassandra-host> 9042
```

### Retrieve Commands

```cql
-- List all keyspaces
DESCRIBE KEYSPACES;

-- Switch to JanusGraph keyspace
USE janusgraph;

-- List all tables in the keyspace
DESCRIBE TABLES;

-- Get full keyspace definition (includes all tables)
DESCRIBE KEYSPACE janusgraph;

-- Get specific table definition
DESCRIBE TABLE janusgraph.edgestore;
DESCRIBE TABLE janusgraph.graphindex;
DESCRIBE TABLE janusgraph.system_properties;

-- Count rows in a table
SELECT COUNT(*) FROM janusgraph.edgestore;
```

---

## Retrieved Keyspace Definition

```cql
CREATE KEYSPACE janusgraph WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
} AND durable_writes = true;
```

> **Note:** For production, use `NetworkTopologyStrategy` with appropriate replication factor.

---

## Retrieved Tables

### 1. edgestore (Core Graph Data)

This is the main table that stores all graph vertices, edges, and properties.

```cql
CREATE TABLE janusgraph.edgestore (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {
        'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 
        'max_threshold': '32', 
        'min_threshold': '4'
    }
    AND compression = {
        'chunk_length_in_kb': '64', 
        'class': 'org.apache.cassandra.io.compress.LZ4Compressor'
    }
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

### 2. graphindex (Composite Index Data)

Stores data for JanusGraph's composite indices.

```cql
CREATE TABLE janusgraph.graphindex (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {
        'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 
        'max_threshold': '32', 
        'min_threshold': '4'
    }
    AND compression = {
        'chunk_length_in_kb': '64', 
        'class': 'org.apache.cassandra.io.compress.LZ4Compressor'
    }
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

### 3. system_properties (Schema Metadata)

Stores JanusGraph schema definitions (property keys, vertex labels, edge labels, indices).

```cql
CREATE TABLE janusgraph.system_properties (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {
        'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 
        'max_threshold': '32', 
        'min_threshold': '4'
    }
    AND compression = {
        'chunk_length_in_kb': '64', 
        'class': 'org.apache.cassandra.io.compress.LZ4Compressor'
    }
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

---

## All JanusGraph Tables in Cassandra

| Table Name | Purpose |
|------------|---------|
| `edgestore` | Core graph data (vertices, edges, properties) |
| `edgestore_lock_` | Distributed locking for edgestore |
| `graphindex` | Composite index data |
| `graphindex_lock_` | Distributed locking for graphindex |
| `system_properties` | JanusGraph schema metadata |
| `system_properties_lock_` | Distributed locking for schema |
| `systemlog` | System transaction log |
| `txlog` | Transaction log |
| `janusgraph_ids` | ID allocation blocks |

---

## JanusGraph Graph Schema (Retrieved via Gremlin)

### Property Keys

| Property Name | Data Type | Description |
|--------------|-----------|-------------|
| `usecase` | String | Multi-tenancy partition key |
| `eventId` | String | Unique event identifier (UUID) |
| `timestamp` | Long | Event timestamp (epoch ms) |
| `activityName` | String | Activity name |
| `appName` | String | Source application |
| `correlationValue` | String | Correlation key value |
| `journeyId` | String | Journey identifier (UUID) |
| `status` | String | Journey status |

### Vertex Labels

| Label | Description |
|-------|-------------|
| `Event` | Process mining event |
| `CorrelationKey` | Links events via shared keys |
| `Journey` | Customer journey |

### Edge Labels

| Label | From | To | Description |
|-------|------|-----|-------------|
| `HAS_CORRELATION` | Event | CorrelationKey | Event has correlation key |
| `PART_OF_JOURNEY` | Event | Journey | Event belongs to journey |

### Composite Indices

| Index Name | Indexed Element | Indexed Properties |
|------------|-----------------|-------------------|
| `byUsecaseEventId` | Event | usecase, eventId |
| `byUsecaseCorrelationValue` | CorrelationKey | usecase, correlationValue |
| `byUsecaseJourneyId` | Journey | usecase, journeyId |
| `eventsByUsecase` | Event | usecase |
| `journeysByUsecase` | Journey | usecase |
| `corrKeysByUsecase` | CorrelationKey | usecase |

---

## Retrieve Graph Schema via Gremlin

```groovy
// Connect to JanusGraph Gremlin Console
:remote connect tinkerpop.server conf/remote.yaml
:remote console

// Get schema management
mgmt = graph.openManagement()

// List all property keys
mgmt.getRelationTypes(PropertyKey.class).each { 
    println(it.name() + " : " + it.dataType().simpleName) 
}

// List all vertex labels
mgmt.getVertexLabels().each { println(it.name()) }

// List all edge labels
mgmt.getRelationTypes(EdgeLabel.class).each { println(it.name()) }

// List all graph indexes
mgmt.getGraphIndexes(Vertex.class).each { idx ->
    println(idx.name() + " -> " + idx.getFieldKeys().collect { it.name() })
}

// Rollback (read-only operation)
mgmt.rollback()
```

---

## Production Recommendations

### Keyspace Configuration for Production

```cql
-- Multi-datacenter setup
CREATE KEYSPACE janusgraph WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc1': 3,
    'dc2': 3
} AND durable_writes = true;
```

### Table Tuning for Production

```cql
-- Optimize compaction for write-heavy workloads
ALTER TABLE janusgraph.edgestore WITH compaction = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
    'sstable_size_in_mb': '160'
};

-- Reduce GC grace for faster cleanup
ALTER TABLE janusgraph.edgestore WITH gc_grace_seconds = 86400;
```
