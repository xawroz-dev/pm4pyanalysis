"""
Neo4j-based Journey Stitching (Multi-Pod Safe, Highly Documented Version)
-------------------------------------------------------------------------

This module implements a concrete `JourneyManager` using Neo4j as the backing
graph database.

High-level idea
===============

We receive *events* coming from multiple applications / services. Each event:

- belongs to one “case/journey” in the business sense (for example: one loan,
  one claim, one customer journey), but
- we do NOT get a single global journey ID from upstream systems.

Instead, events carry one or more *correlation IDs* (like order_id, account_id,
session_id, etc.). We model this as a graph:

- `(:Event)` nodes for each raw event.
- `(:Correlation)` nodes for each correlation ID value.
- `(:Event)-[:HAS_KEY]->(:Correlation)` relationships to connect them.

If two events share any correlation ID, they will be connected via a path like:

    (Event1)-[:HAS_KEY]->(CorrelationX)<-[:HAS_KEY]-(Event2)

If there is a *chain* of such connections (Event1 ↔ CorrA ↔ Event2 ↔ CorrB ↔ Event3),
all of those events logically belong to the same “journey”.

In graph theory terms, we are looking for *connected components* of this
bipartite graph (Events + Correlations). Each connected component = one journey.

We use:
- Neo4j GDS (Graph Data Science) library’s WCC algorithm
  (Weakly Connected Components) to assign each node a `componentId`.
- Then we create exactly one `(:Journey)` node per component.

Multi-Pod Safety
================

Multiple pods (workers) may run `process_events()` at the same time. We want:

- NO duplicate journeys for the same component.
- NO chance that events of the same logical journey end up in *different* journeys.

To achieve that:

1. We create a **deterministic key per component**:

   `journeyKey = minimal correlation.id in that component`

   Every pod that processes that component will compute the same `journeyKey`.

2. We enforce a **unique constraint on `Journey.journeyKey`** and use
   `MERGE (j:Journey {journeyKey: ...})`.

   With Neo4j’s transactional semantics, concurrent MERGEs with the same key
   will serialize: only one Journey is created, the others reuse it.

3. Each `process_events()` run uses its own in-memory GDS graph name:

   `stitching_<random_uuid>`

   That avoids pods stepping on each other’s in-memory GDS graphs.

Terminology
===========

- *Event*: a single log / message from an application.
- *Correlation ID*: some ID that connects multiple events (orderId, customerId, etc.).
- *Connected Component*: a set of nodes where any node can reach any other
  through some path.
- *Journey*: our business-level grouping of events, one per connected component.
"""

import time
import uuid
from neo4j import GraphDatabase
import sys
import os

# Make sure we can import the common interface from the project root.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager


class Neo4jJourneyManager(JourneyManager):
    """
    Concrete implementation of JourneyManager for Neo4j.

    Responsibilities:
    - Create and maintain schema (constraints + indexes).
    - Ingest raw events and wire them to correlation nodes.
    - Run stitching (find connected components, one Journey per component).
    - Provide a lookup `get_journey(event_id)` to see all events in the same journey.
    """

    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize the Neo4j driver.

        :param uri: Bolt URI, e.g. "bolt://localhost:7687"
        :param user: Neo4j username
        :param password: Neo4j password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def setup(self):
        """
        Create schema (constraints + indexes) if they do not already exist.

        We add:
        - Unique constraints so we don't accidentally create duplicate nodes
          for the same logical entity.
        - Indexes to speed up queries on frequently used properties.
        """
        with self.driver.session() as session:
            # Ensure every Event has a globally unique ID.
            session.run("""
                CREATE CONSTRAINT event_id IF NOT EXISTS
                FOR (e:Event) REQUIRE e.id IS UNIQUE
            """)

            # Ensure every Journey has a globally unique ID.
            session.run("""
                CREATE CONSTRAINT journey_id IF NOT EXISTS
                FOR (j:Journey) REQUIRE j.id IS UNIQUE
            """)

            # Ensure every Correlation node has a unique correlation ID.
            session.run("""
                CREATE CONSTRAINT correlation_id IF NOT EXISTS
                FOR (c:Correlation) REQUIRE c.id IS UNIQUE
            """)

            # NEW: deterministic key for one Journey per logical connected component.
            # journeyKey is a *stable* property: for each component, we compute
            # the minimal correlation.id; every pod will compute the same value.
            # This lets us use MERGE safely in a multi-pod environment.
            session.run("""
                CREATE CONSTRAINT journey_key IF NOT EXISTS
                FOR (j:Journey) REQUIRE j.journeyKey IS UNIQUE
            """)

            # Index to quickly find Events by status (NEW vs PROCESSED).
            session.run("""
                CREATE INDEX event_status IF NOT EXISTS
                FOR (e:Event) ON (e.status)
            """)

            # Index to quickly find Correlations by componentId after WCC.
            session.run("""
                CREATE INDEX correlation_component IF NOT EXISTS
                FOR (c:Correlation) ON (c.componentId)
            """)

            # Index to quickly find Events by componentId after WCC.
            session.run("""
                CREATE INDEX event_component IF NOT EXISTS
                FOR (e:Event) ON (e.componentId)
            """)

    def clean(self):
        """
        Wipe the database and re-apply schema.

        Intended for benchmarks or local testing where we want to start
        from a clean slate.
        """
        with self.driver.session() as session:
            # Delete all nodes and relationships.
            session.run("MATCH (n) DETACH DELETE n")

            # Best-effort drop of any old fixed-name GDS graph.
            # (This is mainly for backwards compatibility.)
            try:
                session.run("CALL gds.graph.drop('stitching', false) YIELD graphName")
            except Exception:
                # Ignore if the graph doesn't exist.
                pass

        # Re-create constraints / indexes.
        self.setup()

    def ingest_batch(self, events_batch):
        """
        Ingest a batch of events into the graph.

        Each event in `events_batch` is expected to be a dict like:

            {
                "id": "event_123",
                "payload": {...},            # arbitrary JSON-like structure
                "correlation_ids": ["A", "B", "C"]
            }

        What we create in the graph:

            (e:Event {
                id: <event.id>,
                status: 'NEW',              # we will process NEW events later
                created_at: <timestamp>,
                payload: <stringified payload>
            })

            For each correlation id "cid" in correlation_ids:
            (c:Correlation {id: cid})

            And we connect them:
            (e)-[:HAS_KEY]->(c)

        This builds the bipartite graph:
            Event --HAS_KEY--> Correlation
        which we later analyze using WCC to form journeys.
        """
        if not events_batch:
            return

        # Pre-process payload to a string.
        # Neo4j can store maps, but for simplicity/benchmark we store it as text.
        processed_batch = []
        for ev in events_batch:
            ev_copy = ev.copy()
            ev_copy["payload"] = str(ev.get("payload", {}))
            processed_batch.append(ev_copy)

        with self.driver.session() as session:
            # Cypher explanation:
            #
            # UNWIND $events AS event
            #   Take the list of events passed from Python and treat each one
            #   as a separate row named "event".
            #
            # CREATE (e:Event { ... })
            #   For each row, create an Event node with an id, status, timestamp,
            #   and a serialized payload.
            #
            # WITH e, event
            #   Keep both the new Event node "e" and the original map "event"
            #   for further processing.
            #
            # UNWIND event.correlation_ids AS cid
            #   For this event, iterate through all of its correlation IDs.
            #
            # MERGE (c:Correlation {id: cid})
            #   Ensure we have exactly one Correlation node per ID value across
            #   the whole graph. If it exists, re-use, otherwise create it.
            #
            # MERGE (e)-[:HAS_KEY]->(c)
            #   Create or re-use a HAS_KEY relationship connecting this Event
            #   to the Correlation.
            session.run("""
                UNWIND $events AS event
                CREATE (e:Event {
                    id: event.id,
                    status: 'NEW',
                    created_at: timestamp(),
                    payload: event.payload
                })
                WITH e, event
                UNWIND event.correlation_ids AS cid
                MERGE (c:Correlation {id: cid})
                MERGE (e)-[:HAS_KEY]->(c)
            """, events=processed_batch)

    def process_events(self):
        """
        Run the stitching logic for all NEW events.

        Steps (high level):
        -------------------

        1. Build an in-memory graph for GDS (Graph Data Science) using a
           unique name per run, so multiple pods don't interfere.

        2. Run WCC (Weakly Connected Components) to assign each Event and
           Correlation a `componentId` property. All nodes in the same connected
           component share the same componentId.

        3. Find all componentIds that have at least one NEW Event. Those are the
           "fresh" components we need to attach to journeys.

        4. For each such componentId:
           - Gather its Correlation nodes.
           - Compute a deterministic key: journeyKey = minimal correlation.id.
           - MERGE a Journey node with this journeyKey (unique constraint ensures
             only one Journey per journeyKey).
           - Connect all correlations in this component to that Journey
             via PART_OF relationships.
           - Delete any "loser" Journeys that might still be connected to
             these correlations (from previous runs / merges).
           - Mark all NEW events in the component as PROCESSED.

        5. Drop the in-memory GDS graph to free resources.

        Why this is safe with multiple pods:
        ------------------------------------

        - Each pod uses a different in-memory graph name, so WCC graphs
          do not clash in GDS.

        - The Journey uniqueness is controlled by `journeyKey`, which is
          deterministic per component. All pods compute the same key for the
          same component, and the unique constraint forces a single Journey per
          key.

        - Neo4j serializes concurrent MERGEs with the same key so we don't
          create duplicates.
        """
        # Generate a unique graph name for this run to avoid cross-pod collisions.
        graph_name = f"stitching_{uuid.uuid4().hex}"

        with self.driver.session() as session:
            # ------------------------------------------------------------------
            # 1. Project Graph for WCC with a unique in-memory graph name
            # ------------------------------------------------------------------
            # Best effort: if a graph with this name somehow exists, drop it.
            try:
                session.run("""
                    CALL gds.graph.drop($graphName, false)
                    YIELD graphName AS dropped
                """, graphName=graph_name)
            except Exception:
                # If it doesn't exist, that's fine.
                pass

            # Build the GDS projection:
            #
            # CALL gds.graph.project(
            #     $graphName,
            #     ['Event', 'Correlation'],           -- which labels are nodes
            #     { HAS_KEY: { orientation: 'UNDIRECTED' } } -- which rels & direction
            # )
            #
            # We instruct GDS to treat HAS_KEY relationships as UNDIRECTED,
            # because WCC only cares about connectivity, not direction.
            session.run("""
                CALL gds.graph.project(
                    $graphName,
                    ['Event', 'Correlation'],
                    { HAS_KEY: { orientation: 'UNDIRECTED' } }
                )
            """, graphName=graph_name)

            # ------------------------------------------------------------------
            # 2. Run WCC (Weakly Connected Components)
            # ------------------------------------------------------------------
            #
            # CALL gds.wcc.write(
            #     $graphName,
            #     { writeProperty: 'componentId' }
            # )
            #
            # WCC finds groups of nodes where each node is reachable from any
            # other node via some path. It writes an integer "componentId"
            # onto each node. All nodes in the same group share the same value.
            session.run("""
                CALL gds.wcc.write(
                    $graphName,
                    { writeProperty: 'componentId' }
                )
            """, graphName=graph_name)

            # ------------------------------------------------------------------
            # 3. Find all componentIds that contain NEW events
            # ------------------------------------------------------------------
            #
            # MATCH (e:Event {status: 'NEW'})
            # RETURN DISTINCT e.componentId AS cid
            #
            # We only need to process components that include NEW events.
            result = session.run("""
                MATCH (e:Event {status: 'NEW'})
                RETURN DISTINCT e.componentId AS cid
            """)
            component_ids = [record["cid"] for record in result]

            if not component_ids:
                # No new components to process; drop the in-memory graph and exit.
                session.run("""
                    CALL gds.graph.drop($graphName, false)
                    YIELD graphName AS dropped
                """, graphName=graph_name)
                return

            # We'll handle componentIds in batches to keep transactions reasonable.
            BATCH_SIZE = 100

            for i in range(0, len(component_ids), BATCH_SIZE):
                batch_cids = component_ids[i:i + BATCH_SIZE]

                # ------------------------------------------------------------------
                # 4a. Normalize (or create) Journeys for these components
                # ------------------------------------------------------------------
                #
                # For each componentId (`target_cid`):
                #
                # 1) MATCH (c:Correlation {componentId: target_cid})
                #    -> get all Correlation nodes that belong to this component.
                #
                # 2) WITH target_cid, collect(c) AS correlations, min(c.id) AS journeyKey
                #    -> collect all correlations into a list and compute a deterministic
                #       journeyKey as the minimal correlation id.
                #
                # 3) MERGE (j:Journey {journeyKey: journeyKey})
                #    ON CREATE SET j.id = 'journey_' + randomUUID(), j.created_at = timestamp()
                #
                #    - If a Journey with this journeyKey exists (from another pod or run),
                #      MERGE re-uses it.
                #    - If not, create a new Journey with a random ID and timestamp.
                #
                # 4) FOREACH (c IN correlations | MERGE (c)-[:PART_OF]->(j))
                #
                #    - Ensure each correlation is linked to the canonical Journey
                #      via a PART_OF relationship.
                #
                # 5) OPTIONAL MATCH (c2:Correlation)-[:PART_OF]->(other:Journey)
                #    WHERE c2 IN correlations AND other <> j
                #
                #    - Find any "other" Journey nodes that these correlations were
                #      attached to previously.
                #
                #    WITH j, collect(DISTINCT other) AS losers
                #    FOREACH (loser IN losers | DETACH DELETE loser)
                #
                #    - Remove those loser journeys and all their relationships.
                #      After this step, the correlations for this component will
                #      only be connected to the single canonical Journey `j`.
                session.run("""
                    UNWIND $cids AS target_cid

                    // 1) All Correlation nodes in this component.
                    MATCH (c:Correlation {componentId: target_cid})
                    WITH target_cid, collect(c) AS correlations, min(c.id) AS journeyKey

                    // 2) One Journey per journeyKey, shared across pods.
                    MERGE (j:Journey {journeyKey: journeyKey})
                    ON CREATE SET
                        j.id = 'journey_' + randomUUID(),
                        j.created_at = timestamp()

                    // 3) Attach all correlations in this component to this Journey.
                    FOREACH (c IN correlations | MERGE (c)-[:PART_OF]->(j))

                    // 4) Clean up any other (now redundant) Journeys for these correlations.
                    WITH correlations, j
                    OPTIONAL MATCH (c2:Correlation)-[:PART_OF]->(other:Journey)
                    WHERE c2 IN correlations AND other <> j
                    WITH j, collect(DISTINCT other) AS losers
                    FOREACH (loser IN losers | DETACH DELETE loser)
                """, cids=batch_cids)

                # ------------------------------------------------------------------
                # 4b. Mark NEW events in these components as PROCESSED
                # ------------------------------------------------------------------
                #
                # UNWIND $cids AS target_cid
                # MATCH (e:Event {componentId: target_cid, status: 'NEW'})
                # SET e.status = 'PROCESSED'
                #
                # After stitching, these events no longer need processing. This
                # also prevents us from re-processing the same journey repeatedly.
                session.run("""
                    UNWIND $cids AS target_cid
                    MATCH (e:Event {componentId: target_cid, status: 'NEW'})
                    SET e.status = 'PROCESSED'
                """, cids=batch_cids)

            # ----------------------------------------------------------------------
            # 5. Drop the in-memory GDS graph for this run
            # ----------------------------------------------------------------------
            session.run("""
                CALL gds.graph.drop($graphName, false)
                YIELD graphName AS dropped
            """, graphName=graph_name)

    def get_journey(self, event_id: str):
        """
        Given an event ID, return the journey ID and list of all event IDs
        in the same journey.

        Steps:
        ------

        1. Start from the given Event.
        2. Follow HAS_KEY to its Correlations.
        3. Follow PART_OF to the Journey.
        4. From that Journey, traverse back to all Correlations that belong
           to it, then to all Events that have HAS_KEY to those correlations.

        This gives us all events that share a correlation component with the
        original event (i.e., all events in the same journey).
        """
        with self.driver.session() as session:
            # Cypher explanation:
            #
            # MATCH (e:Event {id: $eid})-[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j:Journey)
            #   - Find the Event with the given ID.
            #   - Go to any Correlation it is connected to via HAS_KEY.
            #   - From those Correlations, go to the Journey via PART_OF.
            #
            # MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(all_e:Event)
            #   - From that Journey, go back to all Correlations that belong to it.
            #   - From those correlations, go to all Events that have HAS_KEY to them.
            #
            # RETURN j.id AS jid, collect(DISTINCT all_e.id) AS event_ids
            #   - Return the Journey ID and the deduplicated list of all event IDs.
            result = session.run("""
                MATCH (e:Event {id: $eid})-[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j:Journey)
                MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(all_e:Event)
                RETURN j.id AS jid, collect(DISTINCT all_e.id) AS event_ids
            """, eid=event_id)

            # We expect at most one Journey per event, so `.single()` is safe.
            record = result.single()
            if record:
                return {
                    "journey_id": record["jid"],
                    "events": record["event_ids"],
                }
            return None


if __name__ == "__main__":
    # Simple benchmark harness using existing helpers.
    from common.generator import generate_traffic
    from common.validator import validate_stitching

    jm = Neo4jJourneyManager("bolt://localhost:7687", "neo4j", "password")

    # Wait for DB to be ready (useful for containerized environments).
    for _ in range(30):
        try:
            jm.setup()
            break
        except Exception:
            time.sleep(2)

    # Start from a clean database for the benchmark.
    jm.clean()

    NUM_JOURNEYS = 1000     # how many logical journeys to generate
    EVENTS_PER_APP = 5    # events per app per journey
    NUM_APPS = 5            # how many different applications/components

    print("Starting Benchmark: Neo4j (WCC Optimized, Multi-Pod Safe)")
    generated_data, ingest_time = generate_traffic(
        jm,
        NUM_JOURNEYS,
        EVENTS_PER_APP,
        NUM_APPS,
    )

    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process

    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")

    # Validate that every sampled event resolves to the correct journey.
    validate_stitching(jm, generated_data)

    jm.close()
