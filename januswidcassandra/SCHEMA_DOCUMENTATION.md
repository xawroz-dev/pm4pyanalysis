# JanusGraph + Cassandra Schema Documentation for DBAs

This document provides the complete schema definition for the Process Mining graph database running on JanusGraph with Cassandra as the storage backend.

## 1. Cassandra Keyspace

JanusGraph automatically creates the keyspace. Default name: `janusgraph`

```cql
-- View keyspace (auto-created by JanusGraph)
DESCRIBE KEYSPACE janusgraph;

-- Expected tables created by JanusGraph:
-- - janusgraph.edgestore          (core graph data)
-- - janusgraph.edgestore_lock_    (locking)
-- - janusgraph.graphindex         (index data)
-- - janusgraph.graphindex_lock_   (locking)
-- - janusgraph.system_properties  (schema metadata)
-- - janusgraph.system_properties_lock_ (locking)
-- - janusgraph.systemlog          (transaction log)
-- - janusgraph.txlog              (transaction log)
```

## 2. Graph Schema (Vertices, Edges, Properties)

### 2.1 Property Keys

| Property Name | Data Type | Cardinality | Description |
|--------------|-----------|-------------|-------------|
| `usecase` | String | SINGLE | Multi-tenancy partition key |
| `eventId` | String | SINGLE | Unique event identifier (UUID) |
| `timestamp` | Long | SINGLE | Event timestamp (epoch milliseconds) |
| `activityName` | String | SINGLE | Name of the activity performed |
| `appName` | String | SINGLE | Source application name |
| `correlationValue` | String | SINGLE | Key for linking events across apps |
| `journeyId` | String | SINGLE | Unique journey identifier (UUID) |
| `status` | String | SINGLE | Journey status (active, completed, etc.) |

### 2.2 Vertex Labels

| Vertex Label | Description | Key Properties |
|-------------|-------------|----------------|
| `Event` | A single process event | usecase, eventId, timestamp, activityName, appName, correlationValue |
| `CorrelationKey` | Links events that belong together | usecase, correlationValue |
| `Journey` | A complete customer journey | usecase, journeyId, status |

### 2.3 Edge Labels

| Edge Label | From Vertex | To Vertex | Description |
|------------|-------------|-----------|-------------|
| `HAS_CORRELATION` | Event | CorrelationKey | Links event to its correlation key |
| `PART_OF_JOURNEY` | Event | Journey | Assigns event to a journey |

## 3. Composite Indices (Critical for Performance)

These indices are **REQUIRED** for efficient queries. Without them, queries will perform full graph scans.

### 3.1 Index Definitions

```groovy
// ============================================================================
// EXECUTE THIS IN GREMLIN CONSOLE TO CREATE SCHEMA
// ============================================================================

mgmt = graph.openManagement()

// --- Property Keys ---
usecase = mgmt.makePropertyKey('usecase').dataType(String.class).make()
eventId = mgmt.makePropertyKey('eventId').dataType(String.class).make()
timestamp = mgmt.makePropertyKey('timestamp').dataType(Long.class).make()
activityName = mgmt.makePropertyKey('activityName').dataType(String.class).make()
appName = mgmt.makePropertyKey('appName').dataType(String.class).make()
correlationValue = mgmt.makePropertyKey('correlationValue').dataType(String.class).make()
journeyId = mgmt.makePropertyKey('journeyId').dataType(String.class).make()
status = mgmt.makePropertyKey('status').dataType(String.class).make()

// --- Vertex Labels ---
eventLabel = mgmt.makeVertexLabel('Event').make()
corrKeyLabel = mgmt.makeVertexLabel('CorrelationKey').make()
journeyLabel = mgmt.makeVertexLabel('Journey').make()

// --- Edge Labels ---
mgmt.makeEdgeLabel('HAS_CORRELATION').make()
mgmt.makeEdgeLabel('PART_OF_JOURNEY').make()

// --- Composite Indices (CRITICAL for performance) ---

// Index 1: Find events by usecase + eventId
mgmt.buildIndex('byUsecaseEventId', Vertex.class)
    .addKey(usecase)
    .addKey(eventId)
    .indexOnly(eventLabel)
    .buildCompositeIndex()

// Index 2: Find correlation keys by usecase + correlationValue
mgmt.buildIndex('byUsecaseCorrelationValue', Vertex.class)
    .addKey(usecase)
    .addKey(correlationValue)
    .indexOnly(corrKeyLabel)
    .buildCompositeIndex()

// Index 3: Find journeys by usecase + journeyId
mgmt.buildIndex('byUsecaseJourneyId', Vertex.class)
    .addKey(usecase)
    .addKey(journeyId)
    .indexOnly(journeyLabel)
    .buildCompositeIndex()

// Index 4: Find all events for a usecase (for cleanup/analytics)
mgmt.buildIndex('eventsByUsecase', Vertex.class)
    .addKey(usecase)
    .indexOnly(eventLabel)
    .buildCompositeIndex()

// Index 5: Find all journeys for a usecase
mgmt.buildIndex('journeysByUsecase', Vertex.class)
    .addKey(usecase)
    .indexOnly(journeyLabel)
    .buildCompositeIndex()

// Index 6: Find all correlation keys for a usecase
mgmt.buildIndex('corrKeysByUsecase', Vertex.class)
    .addKey(usecase)
    .indexOnly(corrKeyLabel)
    .buildCompositeIndex()

mgmt.commit()

// ============================================================================
// WAIT FOR INDEX REGISTRATION (run after commit)
// ============================================================================
mgmt = graph.openManagement()
mgmt.awaitGraphIndexStatus(graph, 'byUsecaseEventId').call()
mgmt.awaitGraphIndexStatus(graph, 'byUsecaseCorrelationValue').call()
mgmt.awaitGraphIndexStatus(graph, 'byUsecaseJourneyId').call()
mgmt.awaitGraphIndexStatus(graph, 'eventsByUsecase').call()
mgmt.awaitGraphIndexStatus(graph, 'journeysByUsecase').call()
mgmt.awaitGraphIndexStatus(graph, 'corrKeysByUsecase').call()
mgmt.rollback()
```

## 4. Cassandra Tables Created by JanusGraph

After JanusGraph creates the schema, these tables will exist in Cassandra:

```cql
-- View all tables
USE janusgraph;
DESCRIBE TABLES;

-- Main data tables:
-- edgestore - stores all graph data (vertices, edges, properties)
-- graphindex - stores index data for composite indices
-- system_properties - stores schema metadata
```

### 4.1 Edgestore Table Structure

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
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND gc_grace_seconds = 864000;
```

## 5. Production Recommendations

### 5.1 Cassandra Cluster Configuration

For production, use a multi-node Cassandra cluster:

```yaml
# Example cassandra.yaml settings
num_tokens: 256
seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      - seeds: "cassandra-node1,cassandra-node2,cassandra-node3"
endpoint_snitch: GossipingPropertyFileSnitch
```

### 5.2 JanusGraph Configuration (janusgraph.properties)

```properties
# Storage Backend
storage.backend=cql
storage.hostname=cassandra-node1,cassandra-node2,cassandra-node3
storage.port=9042
storage.cql.keyspace=janusgraph
storage.cql.local-datacenter=dc1

# Consistency (QUORUM for multi-node)
storage.cql.read-consistency-level=LOCAL_QUORUM
storage.cql.write-consistency-level=LOCAL_QUORUM

# Cache Settings
cache.db-cache=true
cache.db-cache-clean-wait=20
cache.db-cache-time=180000
cache.db-cache-size=0.5

# ID Allocation
ids.block-size=100000
ids.authority.wait-time=300

# Index Backend (use Elasticsearch for production)
index.search.backend=elasticsearch
index.search.hostname=elasticsearch-node1
index.search.port=9200
```

### 5.3 Sizing Guidelines

| Data Volume | Cassandra Nodes | JanusGraph Instances | RAM per Node |
|------------|-----------------|---------------------|--------------|
| < 10M events | 1 | 1 | 8GB |
| 10M - 100M events | 3 | 2 | 16GB |
| 100M - 1B events | 5+ | 3+ | 32GB+ |

## 6. Monitoring Queries

```groovy
// Count vertices by label
g.V().hasLabel('Event').count()
g.V().hasLabel('Journey').count()
g.V().hasLabel('CorrelationKey').count()

// Count by usecase
g.V().has('Event', 'usecase', 'production').count()

// Check index status
mgmt = graph.openManagement()
mgmt.printIndexes()
mgmt.rollback()
```

## 7. Backup Strategy

```bash
# Cassandra snapshot (run on each node)
nodetool snapshot janusgraph

# Copy snapshots from:
# /var/lib/cassandra/data/janusgraph/*/snapshots/
```
