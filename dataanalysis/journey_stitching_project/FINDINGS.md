# Journey Stitching: Cassandra vs Elasticsearch Findings

This document details the findings, advantages, and disadvantages of using Cassandra versus Elasticsearch for high-volume journey stitching, based on architectural analysis and benchmark design.

## 1. Cassandra Approach

### Overview
The Cassandra solution uses a multi-table design (`events`, `correlation_mapping`, `journeys`) and relies on Lightweight Transactions (LWT) (`IF NOT EXISTS`) to handle race conditions when claiming correlation IDs.

### Advantages
1.  **Write Throughput**: Cassandra is a wide-column store optimized for write-heavy workloads. It can handle millions of writes per second with linear scalability.
2.  **Strong Consistency (Selective)**: By using LWT for the critical "claim" operation, we ensure that only one pod creates a journey for a new correlation ID, preventing duplicate journeys.
3.  **Predictable Performance**: Point lookups (by `event_id` or `journey_id`) are extremely fast and constant time.
4.  **Clean Data Management**: Time-To-Live (TTL) features allow automatic expiration of old events, keeping the dataset size manageable without manual cleanup jobs.

### Disadvantages
1.  **LWT Overhead**: LWTs require 4 round-trips (Paxos) and are significantly slower than standard writes. Heavy use of LWT for *every* new correlation key can become a bottleneck.
2.  **Complex Merging**: Merging two journeys requires "Read-Modify-Write" patterns across multiple tables. You have to read all events from the "loser" journey and update them one by one (or in batches) to the "winner" journey. This is expensive.
3.  **Limited Querying**: You cannot easily run ad-hoc queries like "find all events with payload X" without secondary indexes (which have their own issues) or allowing inefficient table scans.
4.  **Schema Rigidity**: You must design your tables based on your queries. Changing the access pattern often requires rewriting the data.

## 2. Elasticsearch Approach

### Overview
The Elasticsearch solution indexes events as documents with a `correlation_ids` array. It uses a separate `locks` index to simulate distributed locking for race conditions and `_update_by_query` for merging.

### Advantages
1.  **Flexible Querying**: The biggest advantage. You can find connections by simply searching `correlation_ids: [A, B]`. No complex mapping tables are strictly required for discovery.
2.  **Rich Analysis**: You can easily search and filter events by any field in the payload, making it excellent for debugging and analytics.
3.  **Simpler "Stitching" Logic**: The "graph" is implicit. If two documents share a correlation ID, they are connected. You can find the journey by querying.

### Disadvantages
1.  **Race Conditions & Consistency**: Elasticsearch is "near real-time". A document indexed now might not be visible for 1 second. This makes it very hard to prevent two pods from creating duplicate journeys simultaneously without external locking (which kills performance).
2.  **Update Costs**: Elasticsearch is immutable. Updating a document (e.g., changing `journey_id` during a merge) requires re-indexing the entire document. `_update_by_query` is a heavy operation that can time out on large datasets.
3.  **Resource Intensive**: Indexing is CPU and memory heavy. Maintaining the inverted index for high-volume ingestion requires significantly more hardware than Cassandra for the same throughput.
4.  **Garbage Collection**: Heavy updates (merges) create deleted documents that need to be merged out by Lucene, causing I/O spikes.

## 3. Benchmark Expectations (Simulated)

If run on a standard cluster (e.g., 3 nodes):

| Metric | Cassandra | Elasticsearch |
| :--- | :--- | :--- |
| **Ingestion Rate** | **High** (~50k+ events/sec) | **Medium** (~10-20k events/sec) |
| **Stitching Latency** | **Low** (<10ms) | **Medium** (>50ms due to refresh/locking) |
| **Merge Performance** | **Slow** (Linear with journey size) | **Very Slow** (Re-indexing overhead) |
| **Scalability** | **Linear** | **Sub-linear** (Indexing overhead grows) |

## 4. Final Recommendation

**Use Cassandra if:**
- You have **massive scale** (>100k events/sec).
- You need **strict correctness** for journey creation (no duplicates).
- Your access patterns are well-defined (Lookup by ID).

**Use Elasticsearch if:**
- You have **moderate scale**.
- You need **search capabilities** on event payloads.
- You value **developer productivity** and flexibility over raw write throughput.
- You can tolerate eventual consistency or occasional duplicate journeys.

**Hybrid Approach (Best of Both)**:
Use **Cassandra** for the "Source of Truth" and high-speed ingestion/stitching. Stream the stitched events to **Elasticsearch** for searching and analytics.
