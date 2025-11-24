import psycopg2
from psycopg2 import pool
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from collections import defaultdict
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ApacheAGE")

# ---------------------------------------------------------------------------
# Interface (same contract as your common.interface.JourneyManager)
# ---------------------------------------------------------------------------


class JourneyManager(ABC):
    @abstractmethod
    def setup(self):
        """Initialize the database/graph and create schema/indexes."""
        pass

    @abstractmethod
    def clean(self):
        """Clean up data to start fresh."""
        pass

    @abstractmethod
    def ingest_batch(self, events_batch: List[Dict[str, Any]]):
        """
        Ingest a batch of events.
        events_batch: list of dicts {'id': str, 'correlation_ids': list, 'payload': dict}
        """
        pass

    @abstractmethod
    def process_events(self):
        """
        Run the stitching logic:
        1. Identify new events.
        2. Cluster/Graph traversal.
        3. Merge/Link journeys.
        """
        pass

    @abstractmethod
    def get_journey(self, event_id: str):
        """
        Return journey details for validation.
        Returns: {'journey_id': str, 'events': list[str]} or None
        """
        pass


# ---------------------------------------------------------------------------
# Traffic generator (same semantics as your common.generator.generate_traffic)
# ---------------------------------------------------------------------------


def generate_traffic(
    jm: JourneyManager,
    num_journeys: int,
    events_per_app: int,
    num_apps: int,
    batch_size: int = 1000,
):
    """
    Generate traffic for benchmarking journey stitching.

    Event correlation strategy:
    - Each journey has a unique base correlation ID
    - Events from the same app within a journey are chained
      (E1[K1], E2[K1,K2], E3[K2,K3]...)
    - Events from different apps in the same journey share the base correlation ID
    - This ensures all events from the same journey (across all apps) get stitched
    """
    total_events = num_journeys * num_apps * events_per_app
    print(
        f"\n[Generator] Generating {num_journeys} journeys "
        f"with {num_apps} apps and {events_per_app} events per app..."
    )
    print(f"[Generator] Total events to generate: {total_events}")

    generated_data: Dict[int, List[str]] = {}
    batch_events: List[Dict[str, Any]] = []

    start_time = time.time()

    for j_idx in range(num_journeys):
        journey_base_key = f"journey_{str(uuid.uuid4())[:8]}"
        event_ids: List[str] = []

        for app_idx in range(num_apps):
            app_name = f"app_{app_idx}"
            app_keys = [
                f"{journey_base_key}_{app_name}_{i}" for i in range(events_per_app)
            ]

            for event_idx in range(events_per_app):
                e_id = f"event_{journey_base_key}_{app_name}_{event_idx}"
                event_ids.append(e_id)

                c_ids = [journey_base_key]
                c_ids.append(app_keys[event_idx])
                if event_idx > 0:
                    c_ids.append(app_keys[event_idx - 1])

                batch_events.append(
                    {
                        "id": e_id,
                        "correlation_ids": c_ids,
                        "payload": {
                            "app": app_name,
                            "journey_index": j_idx,
                            "event_index": event_idx,
                        },
                    }
                )

        generated_data[j_idx] = event_ids

        if len(batch_events) >= batch_size:
            jm.ingest_batch(batch_events)
            batch_events = []

    if batch_events:
        jm.ingest_batch(batch_events)

    duration = time.time() - start_time
    print(f"[Generator] Ingestion complete in {duration:.2f} seconds.")
    return generated_data, duration


# ---------------------------------------------------------------------------
# Apache AGE Journey Manager (optimized)
# ---------------------------------------------------------------------------


class ApacheAgeJourneyManager(JourneyManager):
    """
    Apache AGE Journey Manager with:
    - Connection pooling
    - Bulk SQL ingestion using jsonb_to_agtype / agtype_to_jsonb
    - Cypher-based processing for stitching
    - Indexes on properties and id fields
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        pool_size: int = 10,
        max_workers: int = 4,
        graph_name: str = "benchmark_graph",
    ):
        self.db_config = db_config
        self.graph_name = graph_name
        self.pool_size = pool_size
        self.max_workers = max_workers
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None

        logger.info(
            "Initializing ApacheAgeJourneyManager with pool_size=%s, max_workers=%s",
            pool_size,
            max_workers,
        )
        self._init_connection_pool()

    # ------------------------------------------------------------------ #
    # Connection helpers
    # ------------------------------------------------------------------ #

    def _init_connection_pool(self):
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                minconn=1, maxconn=self.pool_size, **self.db_config
            )
            logger.info("Connection pool initialized (%s max connections)", self.pool_size)
        except Exception as e:
            logger.error("Failed to initialize connection pool: %s", e, exc_info=True)
            raise

    def _get_connection(self):
        if not self.connection_pool:
            raise RuntimeError("Connection pool is not initialized")
        conn = self.connection_pool.getconn()
        conn.autocommit = True
        return conn

    def _return_connection(self, conn):
        try:
            if self.connection_pool and conn:
                self.connection_pool.putconn(conn)
        except Exception as e:
            logger.error("Failed to return connection to pool: %s", e, exc_info=True)

    def _ensure_age_loaded(self, cursor):
        """
        Make sure AGE is loaded and search_path is set.
        We *don't* try to cache flags on the connection object anymore.
        """
        cursor.execute("LOAD 'age';")
        cursor.execute("SET search_path = ag_catalog, '$user', public;")

    # ------------------------------------------------------------------ #
    # Cypher execution helper
    # ------------------------------------------------------------------ #

    def _execute_cypher(
        self,
        query: str,
        cols: str = "v agtype",
        conn=None,
        retry_count: int = 3,
    ) -> List[Tuple]:
        own_conn = False
        if conn is None:
            conn = self._get_connection()
            own_conn = True

        attempt = 0
        last_error: Optional[Exception] = None

        while attempt < retry_count:
            try:
                with conn.cursor() as cursor:
                    self._ensure_age_loaded(cursor)

                    full_query = (
                        f"SELECT * FROM cypher('{self.graph_name}', $$ {query} $$) "
                        f"AS ({cols});"
                    )

                    start = time.time()
                    cursor.execute(full_query)

                    try:
                        rows = cursor.fetchall()
                    except psycopg2.ProgrammingError:
                        rows = []

                    elapsed = time.time() - start
                    if elapsed > 1.0:
                        logger.warning(
                            "Slow Cypher query (%.2fs):\n%s",
                            elapsed,
                            query[:200] + ("..." if len(query) > 200 else ""),
                        )
                    return rows

            except psycopg2.OperationalError as e:
                attempt += 1
                last_error = e
                logger.warning(
                    "Cypher query failed (attempt %s/%s): %s",
                    attempt,
                    retry_count,
                    e,
                )
                time.sleep(0.1 * attempt)
            except Exception as e:
                logger.error("Unexpected Cypher error: %s", e, exc_info=True)
                raise
            finally:
                if own_conn and attempt >= retry_count:
                    self._return_connection(conn)

        if own_conn:
            self._return_connection(conn)

        logger.error("Cypher query failed after %s attempts: %s", retry_count, last_error)
        if last_error:
            raise last_error
        raise RuntimeError("Unknown Cypher failure")

    # ------------------------------------------------------------------ #
    # Graph setup / clean / indexes
    # ------------------------------------------------------------------ #

    def setup(self):
        """
        Create extension, graph, labels and indexes.
        Idempotent: safe to call multiple times.
        """
        logger.info("Setting up graph %s...", self.graph_name)
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                # Extension + basic AGE setup
                cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
                self._ensure_age_loaded(cursor)

                # Create graph if needed
                cursor.execute(
                    "SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,)
                )
                exists = cursor.fetchone()[0] > 0
                if not exists:
                    cursor.execute("SELECT create_graph(%s);", (self.graph_name,))
                    logger.info("Created graph: %s", self.graph_name)
                else:
                    logger.info("Graph %s already exists", self.graph_name)

            # Ensure labels exist (cheap, once)
            conn2 = self._get_connection()
            try:
                for label in ("Event", "Correlation", "Journey"):
                    # CREATE is idempotent wrt label tables: first use creates them
                    self._execute_cypher(
                        f"CREATE (:{label}) RETURN 1", cols="dummy agtype", conn=conn2
                    )
            finally:
                self._return_connection(conn2)

            # Create indexes on underlying label tables
            self._create_indexes()

            logger.info("Setup complete (graph + labels + indexes).")

        except Exception as e:
            logger.error("Setup failed: %s", e, exc_info=True)
            raise
        finally:
            self._return_connection(conn)

    def _create_indexes(self):
        """Create GIN index on properties and btree index on id property."""
        logger.info("Ensuring indexes on Event/Correlation/Journey...")
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                self._ensure_age_loaded(cursor)

                labels = ("Event", "Correlation", "Journey")
                for label in labels:
                    table = f'"{self.graph_name}"."{label}"'

                    # GIN index on whole properties map (used by property lookups)
                    gin_idx = f'{label}_properties_idx'
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS "{gin_idx}" '
                        f"ON {table} USING GIN (properties);"
                    )

                    # B-tree index on properties->>'id' (via agtype_to_jsonb)
                    id_idx = f'{label}_id_idx'
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS "{id_idx}" '
                        f'ON {table} ((ag_catalog.agtype_to_jsonb(properties)->>\'id\'));'
                    )

        except Exception as e:
            logger.warning("Index creation failed (non-fatal): %s", e, exc_info=True)
        finally:
            self._return_connection(conn)

    def clean(self):
        """Drop the graph (if exists)."""
        logger.info("Cleaning graph %s...", self.graph_name)
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                self._ensure_age_loaded(cursor)
                try:
                    cursor.execute(
                        "SELECT drop_graph(%s, true);", (self.graph_name,)
                    )
                    logger.info("Dropped graph: %s", self.graph_name)
                except psycopg2.Error as e:
                    # If graph doesn't exist, ignore
                    logger.warning("drop_graph failed (ignored): %s", e)
        finally:
            self._return_connection(conn)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _to_cypher_list(self, data: List[Dict]) -> str:
        """
        Convert list[dict] -> Cypher literal list of maps.
        AGE requires unquoted keys.
        """
        json_str = json.dumps(data)
        cypher_str = json_str.replace("'", "''")
        cypher_str = cypher_str.replace('"', '"')
        cypher_str = cypher_str.replace('"', '"')
        # More robust: "key": -> key:
        cypher_str = json_str
        cypher_str = cypher_str.replace("'", "''")
        cypher_str = cypher_str.replace('\\"', '"')
        cypher_str = cypher_str.replace("\\\\", "\\")
        # Replace "key": with key:
        cypher_str = cypher_str.replace('"id":', "id:")
        cypher_str = cypher_str.replace('"status":', "status:")
        cypher_str = cypher_str.replace('"created_at":', "created_at:")
        cypher_str = cypher_str.replace('"payload":', "payload:")
        cypher_str = cypher_str.replace('"correlation_ids":', "correlation_ids:")
        return cypher_str

    # ------------------------------------------------------------------ #
    # Ingestion (optimized, SQL first; Cypher fallback)
    # ------------------------------------------------------------------ #

    def ingest_batch(self, events_batch: List[Dict[str, Any]]):
        """
        Ingest a batch of events:
        - Bulk INSERT into "graph"."Event" using jsonb_to_agtype
        - Bulk INSERT into "graph"."Correlation" (deduped)
        - Bulk INSERT into "graph"."HAS_KEY" edges
        Falls back to Cypher-based UNWIND ingestion if something fails.
        """
        if not events_batch:
            return

        batch_size = len(events_batch)
        logger.info("Ingesting batch of %d events...", batch_size)
        start_time = time.time()

        # Build JSON payload for SQL
        now_str = datetime.now(timezone.utc).isoformat()
        events_data: List[Dict[str, Any]] = []

        for ev in events_batch:
            events_data.append(
                {
                    "id": ev["id"],
                    "status": "NEW",
                    "created_at": now_str,
                    "payload": ev.get("payload", {}),
                    "correlation_ids": ev["correlation_ids"],
                }
            )

        events_json = json.dumps(events_data)

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                self._ensure_age_loaded(cursor)

                # 1) Insert Event vertices
                sql_events = f"""
                    WITH batch AS (
                        SELECT jsonb_array_elements(%s::jsonb) AS ev
                    )
                    INSERT INTO "{self.graph_name}"."Event"(properties)
                    SELECT ag_catalog.jsonb_to_agtype(ev - 'correlation_ids')
                    FROM batch;
                """
                cursor.execute(sql_events, (events_json,))

                # 2) Insert Correlation vertices (dedup by cid)
                sql_corr = f"""
                    WITH batch AS (
                        SELECT jsonb_array_elements(%s::jsonb) AS ev
                    ),
                    cid AS (
                        SELECT DISTINCT jsonb_array_elements_text(ev->'correlation_ids') AS cid
                        FROM batch
                    ),
                    existing AS (
                        SELECT ag_catalog.agtype_to_jsonb(properties)->>'id' AS cid
                        FROM "{self.graph_name}"."Correlation"
                        WHERE ag_catalog.agtype_to_jsonb(properties)->>'id'
                              IN (SELECT cid FROM cid)
                    ),
                    to_insert AS (
                        SELECT cid
                        FROM cid
                        WHERE cid NOT IN (SELECT cid FROM existing)
                    )
                    INSERT INTO "{self.graph_name}"."Correlation"(properties)
                    SELECT ag_catalog.jsonb_to_agtype(
                        jsonb_build_object('id', cid)
                    )
                    FROM to_insert;
                """
                cursor.execute(sql_corr, (events_json,))

                # 3) Insert HAS_KEY edges
                sql_edges = f"""
                    WITH batch AS (
                        SELECT jsonb_array_elements(%s::jsonb) AS ev
                    ),
                    ev_rows AS (
                        SELECT (ev->>'id') AS event_id,
                               ev->'correlation_ids' AS cids
                        FROM batch
                    ),
                    ec_pairs AS (
                        SELECT event_id,
                               jsonb_array_elements_text(cids) AS cid
                        FROM ev_rows
                    ),
                    event_vertex AS (
                        SELECT ag_catalog.agtype_to_jsonb(properties)->>'id' AS event_id,
                               id AS vid
                        FROM "{self.graph_name}"."Event"
                        WHERE ag_catalog.agtype_to_jsonb(properties)->>'id'
                              IN (SELECT DISTINCT event_id FROM ec_pairs)
                    ),
                    corr_vertex AS (
                        SELECT ag_catalog.agtype_to_jsonb(properties)->>'id' AS cid,
                               id AS vid
                        FROM "{self.graph_name}"."Correlation"
                        WHERE ag_catalog.agtype_to_jsonb(properties)->>'id'
                              IN (SELECT DISTINCT cid FROM ec_pairs)
                    ),
                    pairs AS (
                        SELECT DISTINCT e.vid AS start_id,
                                        c.vid AS end_id
                        FROM ec_pairs p
                        JOIN event_vertex e ON e.event_id = p.event_id
                        JOIN corr_vertex c ON c.cid = p.cid
                    ),
                    existing_edges AS (
                        SELECT start_id, end_id
                        FROM "{self.graph_name}"."HAS_KEY"
                        WHERE (start_id, end_id) IN (SELECT start_id, end_id FROM pairs)
                    ),
                    to_insert AS (
                        SELECT p.start_id, p.end_id
                        FROM pairs p
                        LEFT JOIN existing_edges e
                          ON e.start_id = p.start_id AND e.end_id = p.end_id
                        WHERE e.start_id IS NULL
                    )
                    INSERT INTO "{self.graph_name}"."HAS_KEY"(start_id, end_id, properties)
                    SELECT start_id, end_id, ag_catalog.agtype_build_map()
                    FROM to_insert;
                """
                cursor.execute(sql_edges, (events_json,))

            elapsed = time.time() - start_time
            throughput = batch_size / elapsed if elapsed > 0 else 0.0
            logger.info(
                "Batch ingestion done: %d events in %.2fs (%.0f events/sec)",
                batch_size,
                elapsed,
                throughput,
            )

        except Exception as e:
            logger.error(
                "Batch ingestion failed in SQL path (%s). "
                "Falling back to Cypher UNWIND ingestion for this batch.",
                e,
                exc_info=True,
            )
            self._return_connection(conn)
            self._ingest_batch_cypher(events_batch)
            return
        finally:
            self._return_connection(conn)

    def _ingest_batch_cypher(self, events_batch: List[Dict[str, Any]]):
        """
        Fallback ingestion using Cypher UNWIND + CREATE/MERGE.
        Slower, but safe and simple.
        """
        if not events_batch:
            return

        batch_size = len(events_batch)
        start_time = time.time()
        now_str = datetime.now(timezone.utc).isoformat()

        batch_data = []
        for ev in events_batch:
            batch_data.append(
                {
                    "id": ev["id"],
                    "status": "NEW",
                    "created_at": now_str,
                    "payload": ev.get("payload", {}),
                    "correlation_ids": ev["correlation_ids"],
                }
            )

        batch_cypher = self._to_cypher_list(batch_data)
        conn = self._get_connection()
        try:
            # Create Event nodes
            query_events = f"""
                UNWIND {batch_cypher} AS row
                CREATE (:Event {{
                    id: row.id,
                    status: row.status,
                    created_at: row.created_at,
                    payload: row.payload
                }})
            """
            self._execute_cypher(query_events, conn=conn)

            # Create Correlation + HAS_KEY edges
            query_edges = f"""
                UNWIND {batch_cypher} AS row
                MATCH (e:Event {{id: row.id}})
                UNWIND row.correlation_ids AS cid
                MERGE (c:Correlation {{id: cid}})
                MERGE (e)-[:HAS_KEY]->(c)
            """
            self._execute_cypher(query_edges, conn=conn)

            elapsed = time.time() - start_time
            throughput = batch_size / elapsed if elapsed > 0 else 0.0
            logger.info(
                "[Fallback Cypher] Batch ingestion done: %d events in %.2fs (%.0f events/sec)",
                batch_size,
                elapsed,
                throughput,
            )
        finally:
            self._return_connection(conn)

    # ------------------------------------------------------------------ #
    # Processing (same union-find logic as your original)
    # ------------------------------------------------------------------ #

    def _process_batch_worker(
        self,
        events: List[Dict[str, Any]],
        all_cids: set,
        cid_to_journeys: Dict[str, List[str]],
        existing_journeys_map: Dict[str, str],
    ) -> Dict[str, Any]:
        parent: Dict[str, str] = {}

        def find(x: str) -> str:
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(a: str, b: str):
            ra = find(a)
            rb = find(b)
            if ra != rb:
                parent[ra] = rb

        # Union CIDs within each event; union with their existing journeys
        for ev in events:
            cids = ev["cids"]
            if not cids:
                continue
            first = cids[0]
            for other in cids[1:]:
                union(first, other)

            for cid in cids:
                if cid in cid_to_journeys:
                    for jid in cid_to_journeys[cid]:
                        union(cid, jid)

        groups: Dict[str, Dict[str, set]] = defaultdict(
            lambda: {"cids": set(), "jids": set()}
        )

        for cid in all_cids:
            root = find(cid)
            groups[root]["cids"].add(cid)

        for jid in existing_journeys_map.keys():
            if jid in parent:
                root = find(jid)
                groups[root]["jids"].add(jid)

        new_journeys = []
        merges = []
        cid_links = []

        for root, group in groups.items():
            jids = list(group["jids"])
            target_jid: Optional[str] = None

            if not jids:
                target_jid = f"journey_{uuid.uuid4()}"
                created_at = datetime.now(timezone.utc).isoformat()
                new_journeys.append({"id": target_jid, "created_at": created_at})
            elif len(jids) == 1:
                target_jid = jids[0]
            else:
                # Merge -> keep oldest
                jids.sort(key=lambda j: existing_journeys_map[j])
                target_jid = jids[0]
                for loser in jids[1:]:
                    merges.append({"winner": target_jid, "loser": loser})

            for cid in group["cids"]:
                cid_links.append({"jid": target_jid, "cid": cid})

        return {
            "new_journeys": new_journeys,
            "merges": merges,
            "cid_links": cid_links,
            "event_ids": [e["id"] for e in events],
        }

    def process_events(self):
        """Process NEW events and stitch journeys."""
        logger.info("Starting event processing...")
        total_processed = 0
        batch_count = 0
        start_time = time.time()
        BATCH_SIZE = 5000

        while True:
            batch_start = time.time()
            conn = self._get_connection()

            try:
                # Fetch NEW events + correlations
                fetch_query = f"""
                    MATCH (e:Event {{status: 'NEW'}})
                    WITH e LIMIT {BATCH_SIZE}
                    MATCH (e)-[:HAS_KEY]->(c:Correlation)
                    RETURN e.id, collect(c.id)
                """
                rows = self._execute_cypher(
                    fetch_query, cols="id agtype, cids agtype", conn=conn
                )

                if not rows:
                    logger.info("No more NEW events to process.")
                    break

                events = []
                all_cids = set()
                for row in rows:
                    eid = json.loads(row[0])
                    cids = json.loads(row[1])
                    events.append({"id": eid, "cids": cids})
                    all_cids.update(cids)

                if not all_cids:
                    continue

                logger.info(
                    "Processing batch %d: %d events, %d unique correlations",
                    batch_count + 1,
                    len(events),
                    len(all_cids),
                )

                # Bulk lookup existing journeys
                all_cids_list = list(all_cids)
                cids_json = json.dumps(all_cids_list).replace("'", "''")

                find_journey_query = f"""
                    MATCH (c:Correlation)-[:PART_OF]->(j:Journey)
                    WHERE c.id IN {cids_json}
                    RETURN c.id, j.id, j.created_at
                """
                j_rows = self._execute_cypher(
                    find_journey_query,
                    cols="cid agtype, jid agtype, created_at agtype",
                    conn=conn,
                )

                cid_to_journeys: Dict[str, List[str]] = defaultdict(list)
                existing_journeys_map: Dict[str, str] = {}

                if j_rows:
                    for row in j_rows:
                        cid = json.loads(row[0])
                        jid = json.loads(row[1])
                        created_at = json.loads(row[2])
                        cid_to_journeys[cid].append(jid)
                        existing_journeys_map[jid] = created_at

                # Compute actions
                actions = self._process_batch_worker(
                    events, all_cids, cid_to_journeys, existing_journeys_map
                )

                logger.info(
                    "Batch actions: %d new journeys, %d merges, %d cid links",
                    len(actions["new_journeys"]),
                    len(actions["merges"]),
                    len(actions["cid_links"]),
                )

                # A) Create new journeys
                if actions["new_journeys"]:
                    batch_cypher = self._to_cypher_list(actions["new_journeys"])
                    query = f"""
                        UNWIND {batch_cypher} AS row
                        CREATE (j:Journey {{id: row.id, created_at: row.created_at}})
                    """
                    self._execute_cypher(query, conn=conn)

                # B) Merges
                for merge in actions["merges"]:
                    try:
                        query = f"""
                            MATCH (loser:Journey {{id: '{merge['loser']}'}})
                            MATCH (winner:Journey {{id: '{merge['winner']}'}})
                            OPTIONAL MATCH (c:Correlation)-[r:PART_OF]->(loser)
                            DELETE r
                            WITH c, winner
                            WHERE c IS NOT NULL
                            MERGE (c)-[:PART_OF]->(winner)
                        """
                        self._execute_cypher(query, conn=conn)

                        delete_query = (
                            f"MATCH (j:Journey {{id: '{merge['loser']}'}}) DELETE j"
                        )
                        self._execute_cypher(delete_query, conn=conn)
                    except Exception as e:
                        logger.warning("Merge conflict handled: %s", e)

                # C) Link CIDs to journeys
                if actions["cid_links"]:
                    batch_cypher = self._to_cypher_list(actions["cid_links"])
                    query = f"""
                        UNWIND {batch_cypher} AS row
                        MATCH (j:Journey {{id: row.jid}})
                        MERGE (c:Correlation {{id: row.cid}})
                        MERGE (c)-[:PART_OF]->(j)
                    """
                    self._execute_cypher(query, conn=conn)

                # D) Mark events as PROCESSED
                event_ids = [{"id": eid} for eid in actions["event_ids"]]
                batch_cypher = self._to_cypher_list(event_ids)
                query = f"""
                    UNWIND {batch_cypher} AS row
                    MATCH (e:Event {{id: row.id}})
                    SET e.status = 'PROCESSED'
                """
                self._execute_cypher(query, conn=conn)

                total_processed += len(events)
                batch_count += 1

                batch_elapsed = time.time() - batch_start
                batch_throughput = (
                    len(events) / batch_elapsed if batch_elapsed > 0 else 0.0
                )

                logger.info(
                    "Batch %d complete: %d events in %.2fs (%.0f events/sec)",
                    batch_count,
                    len(events),
                    batch_elapsed,
                    batch_throughput,
                )

            except Exception as e:
                logger.error("Batch processing failed: %s", e, exc_info=True)
                raise
            finally:
                self._return_connection(conn)

        total_elapsed = time.time() - start_time
        overall_throughput = (
            total_processed / total_elapsed if total_elapsed > 0 else 0.0
        )
        logger.info(
            "Event processing complete: %d events in %.2fs (%.0f events/sec, %d batches)",
            total_processed,
            total_elapsed,
            overall_throughput,
            batch_count,
        )

    # ------------------------------------------------------------------ #
    # Journey retrieval
    # ------------------------------------------------------------------ #

    def get_journey(self, event_id: str) -> Optional[Dict[str, Any]]:
        query = f"""
            MATCH (start_e:Event {{id: '{event_id}'}})
                  -[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j:Journey)
            MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(all_e:Event)
            RETURN j.id, collect(DISTINCT all_e.id)
        """
        conn = self._get_connection()
        try:
            rows = self._execute_cypher(
                query, cols="jid agtype, events agtype", conn=conn
            )
            if rows:
                return {
                    "journey_id": json.loads(rows[0][0]),
                    "events": json.loads(rows[0][1]),
                }
            return None
        finally:
            self._return_connection(conn)

    def close(self):
        """Close all connections."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Connection pool closed")


# ---------------------------------------------------------------------------
# Main benchmark runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # If you already have common.validator, keep this import;
    # otherwise, comment it out or implement a trivial validator.
    try:
        from common.validator import validate_stitching  # type: ignore
    except ImportError:
        def validate_stitching(jm, generated_data):
            logger.info(
                "validate_stitching not available; skipping stitching validation."
            )

    DB_CONFIG = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": 5436,
    }

    jm = ApacheAgeJourneyManager(DB_CONFIG, pool_size=10, max_workers=4)

    # Wait for DB to be ready
    logger.info("Waiting for database...")
    for i in range(30):
        try:
            conn = jm._get_connection()
            jm._return_connection(conn)
            logger.info("Database connection OK")
            break
        except Exception:
            time.sleep(2)
    else:
        raise RuntimeError("Could not connect to database")

    # Fresh graph
    jm.clean()
    jm.setup()

    NUM_JOURNEYS = 1000
    EVENTS_PER_APP = 30
    NUM_APPS = 5

    logger.info(
        "Starting Benchmark: Apache AGE (Production-Optimized Ingestion)\n"
        "Configuration: %d journeys, %d apps, %d events/app",
        NUM_JOURNEYS,
        NUM_APPS,
        EVENTS_PER_APP,
    )

    generated_data, ingest_time = generate_traffic(
        jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS
    )

    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process

    total_events = NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP
    total_time = ingest_time + process_time

    logger.info("=" * 60)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 60)
    logger.info("Total Events: %d", total_events)
    logger.info(
        "Ingestion Time: %.2fs (%.0f events/sec)",
        ingest_time,
        total_events / ingest_time if ingest_time > 0 else 0.0,
    )
    logger.info(
        "Processing Time: %.2fs (%.0f events/sec)",
        process_time,
        total_events / process_time if process_time > 0 else 0.0,
    )
    logger.info(
        "Total Time: %.2fs (%.0f events/sec)",
        total_time,
        total_events / total_time if total_time > 0 else 0.0,
    )
    logger.info("=" * 60)

    logger.info("Validating journey stitching...")
    validate_stitching(jm, generated_data)

    jm.close()
    logger.info("Benchmark complete")
