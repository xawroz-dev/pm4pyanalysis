import psycopg2
from psycopg2 import pool, extras
import json
import time
import re
import uuid
from datetime import datetime
from collections import defaultdict
import sys
import os
import logging
import threading
from typing import List, Dict, Any, Optional, Tuple, Set

# Add common directory to path to import interface
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('ApacheAGE')


class ApacheAgeJourneyManager(JourneyManager):
    """
    Production-optimized Apache AGE Journey Manager with:
    - Connection pooling for better resource management
    - Batched processing for improved throughput
    - Race condition handling for multi-pod deployments
    - Comprehensive logging for observability
    """

    def __init__(self, db_config: Dict[str, Any], pool_size: int = 10, max_workers: int = 4):
        """
        Initialize the Journey Manager with connection pooling.

        Args:
            db_config: Database connection configuration
            pool_size: Number of connections in the pool
            max_workers: Number of parallel workers for processing (not yet used)
        """
        self.db_config = db_config
        self.graph_name = "benchmark_graph"
        self.pool_size = pool_size
        self.max_workers = max_workers
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self._lock = threading.Lock()

        # Track which connections have had AGE loaded + search_path set
        self._initialized_connections: Set[int] = set()

        logger.info(
            f"Initializing ApacheAgeJourneyManager with pool_size={pool_size}, "
            f"max_workers={max_workers}"
        )
        self._init_connection_pool()

    # -------------------------------------------------------------------------
    # Connection / AGE init helpers
    # -------------------------------------------------------------------------

    def _init_connection_pool(self):
        """Initialize the connection pool."""
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size,
                **self.db_config
            )
            logger.info(f"Connection pool initialized successfully with {self.pool_size} connections")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}", exc_info=True)
            raise

    def _get_connection(self) -> psycopg2.extensions.connection:
        """Get a connection from the pool and enable autocommit."""
        if self.connection_pool is None:
            raise RuntimeError("Connection pool is not initialized")

        try:
            conn = self.connection_pool.getconn()
            conn.autocommit = True
            return conn
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}", exc_info=True)
            raise

    def _return_connection(self, conn: Optional[psycopg2.extensions.connection]):
        """Return a connection to the pool."""
        if self.connection_pool is None or conn is None:
            return
        try:
            self.connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to return connection to pool: {e}", exc_info=True)

    def _ensure_age_loaded(self, conn: psycopg2.extensions.connection):
        """
        Ensure AGE is loaded and the search_path is set for this connection.
        This is done at most once per connection.
        """
        conn_id = id(conn)
        if conn_id in self._initialized_connections:
            return

        try:
            with conn.cursor() as cursor:
                cursor.execute("LOAD 'age';")
                cursor.execute("SET search_path = ag_catalog, '$user', public;")
            self._initialized_connections.add(conn_id)
            logger.debug(f"AGE loaded and search_path set for connection {conn_id}")
        except Exception as e:
            logger.error(f"Failed to LOAD 'age' or set search_path: {e}", exc_info=True)
            raise

    # -------------------------------------------------------------------------
    # Cypher execution
    # -------------------------------------------------------------------------

    def _execute_cypher(
        self,
        query: str,
        params: Optional[Dict] = None,
        cols: str = "v agtype",
        conn: Optional[psycopg2.extensions.connection] = None,
        retry_count: int = 3
    ) -> List[Tuple]:
        """
        Execute a Cypher query with retry logic for handling transient failures.

        Args:
            query: Cypher query string
            params: Query parameters (not used in current AGE implementation)
            cols: Column specification for result set
            conn: Optional connection to use (otherwise gets from pool)
            retry_count: Number of retries on failure

        Returns:
            List of result tuples
        """
        external_conn = conn is not None
        if not external_conn:
            conn = self._get_connection()

        assert conn is not None  # for type checkers

        # Ensure AGE is loaded and search_path set once per connection
        self._ensure_age_loaded(conn)

        attempt = 0
        last_error: Optional[Exception] = None

        try:
            while attempt < retry_count:
                try:
                    with conn.cursor() as cursor:
                        full_query = (
                            f"SELECT * FROM cypher('{self.graph_name}', $$ {query} $$) "
                            f"as ({cols});"
                        )

                        start_time = time.time()
                        cursor.execute(full_query)

                        try:
                            results = cursor.fetchall()
                        except psycopg2.ProgrammingError:
                            # No rows to fetch (e.g., write-only queries)
                            results = []

                        elapsed = time.time() - start_time
                        if elapsed > 1.0:  # Log slow queries
                            snippet = query.replace("\n", " ")[:200]
                            logger.warning(f"Slow query detected ({elapsed:.2f}s): {snippet}...")

                        return results

                except psycopg2.OperationalError as e:
                    last_error = e
                    attempt += 1
                    logger.warning(f"Cypher query failed (attempt {attempt}/{retry_count}): {e}")

                    if attempt < retry_count:
                        time.sleep(0.1 * attempt)  # simple backoff

                except Exception as e:
                    logger.error(f"Unexpected error executing Cypher query: {e}", exc_info=True)
                    raise

            # If we get here, all retries failed
            logger.error(f"Cypher query failed after {retry_count} attempts: {last_error}")
            if last_error:
                raise last_error
            raise RuntimeError("Cypher query failed after retries with unknown error")

        finally:
            if not external_conn:
                self._return_connection(conn)

    # -------------------------------------------------------------------------
    # Graph setup / cleanup
    # -------------------------------------------------------------------------

    def setup(self):
        """
        Initialize the graph schema with optimized indices.

        - Creates AGE extension (if not already present)
        - Creates the graph (if not exists)
        - Ensures vertex and edge labels exist
        - Adds indexes on vertex/edge columns and properties
        """
        logger.info("Setting up graph schema and indices...")
        conn = self._get_connection()

        try:
            with conn.cursor() as cursor:
                # Ensure extension exists
                cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")

            # Ensure AGE is loaded & search path set
            self._ensure_age_loaded(conn)

            with conn.cursor() as cursor:
                # Create graph if not exists
                cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
                exists = cursor.fetchone()[0] > 0

                if not exists:
                    cursor.execute("SELECT create_graph(%s);", (self.graph_name,))
                    logger.info(f"Created graph: {self.graph_name}")
                else:
                    logger.info(f"Graph already exists: {self.graph_name}")

            # Ensure vertex labels exist
            with conn.cursor() as cursor:
                for vlabel in ["Event", "Correlation", "Journey"]:
                    try:
                        cursor.execute("SELECT create_vlabel(%s, %s);", (self.graph_name, vlabel))
                        logger.info(f"Created vlabel: {vlabel}")
                    except Exception as e:
                        # Label may already exist; that's fine
                        logger.debug(f"create_vlabel({vlabel}) may already exist: {e}")

                # Ensure edge labels exist (for HAS_KEY and PART_OF)
                for elabel in ["HAS_KEY", "PART_OF"]:
                    try:
                        cursor.execute("SELECT create_elabel(%s, %s);", (self.graph_name, elabel))
                        logger.info(f"Created elabel: {elabel}")
                    except Exception as e:
                        logger.debug(f"create_elabel({elabel}) may already exist: {e}")

            # Create optimized indexes
            indices_created = 0
            with conn.cursor() as cursor:
                # Vertex indexes per label
                for label in ["Event", "Correlation", "Journey"]:
                    try:
                        # BTREE on internal id
                        idx_id_col = (
                            f'CREATE INDEX IF NOT EXISTS "idx_{label}_id_col" '
                            f'ON "{self.graph_name}"."{label}" '
                            f'USING BTREE (id);'
                        )
                        cursor.execute(idx_id_col)
                        indices_created += 1
                        logger.info(f"Created/ensured BTREE index on {label}.id")

                        # GIN on properties for property lookups
                        idx_props = (
                            f'CREATE INDEX IF NOT EXISTS "idx_{label}_props_gin" '
                            f'ON "{self.graph_name}"."{label}" '
                            f'USING GIN (properties);'
                        )
                        cursor.execute(idx_props)
                        indices_created += 1
                        logger.info(f"Created/ensured GIN index on {label}.properties")

                    except Exception as e:
                        logger.warning(f"Could not create vertex indexes for {label}: {e}")
                        conn.rollback()

                # Edge indexes per label (HAS_KEY, PART_OF)
                for elabel in ["HAS_KEY", "PART_OF"]:
                    try:
                        # BTREE on id
                        idx_e_id = (
                            f'CREATE INDEX IF NOT EXISTS "idx_{elabel}_id_col" '
                            f'ON "{self.graph_name}"."{elabel}" '
                            f'USING BTREE (id);'
                        )
                        cursor.execute(idx_e_id)
                        indices_created += 1

                        # GIN on properties (if you ever store edge props)
                        idx_e_props = (
                            f'CREATE INDEX IF NOT EXISTS "idx_{elabel}_props_gin" '
                            f'ON "{self.graph_name}"."{elabel}" '
                            f'USING GIN (properties);'
                        )
                        cursor.execute(idx_e_props)
                        indices_created += 1

                        # BTREE on start_id / end_id
                        idx_start = (
                            f'CREATE INDEX IF NOT EXISTS "idx_{elabel}_start_id" '
                            f'ON "{self.graph_name}"."{elabel}" '
                            f'USING BTREE (start_id);'
                        )
                        cursor.execute(idx_start)
                        indices_created += 1

                        idx_end = (
                            f'CREATE INDEX IF NOT EXISTS "idx_{elabel}_end_id" '
                            f'ON "{self.graph_name}"."{elabel}" '
                            f'USING BTREE (end_id);'
                        )
                        cursor.execute(idx_end)
                        indices_created += 1

                        logger.info(
                            f"Created/ensured indexes on {elabel}.id, {elabel}.properties, "
                            f"{elabel}.start_id, {elabel}.end_id"
                        )
                    except Exception as e:
                        logger.warning(f"Could not create edge indexes for {elabel}: {e}")
                        conn.rollback()

            logger.info(f"Setup complete. Ensured {indices_created} indexes are present.")

        except Exception as e:
            logger.error(f"Setup failed: {e}", exc_info=True)
            raise
        finally:
            self._return_connection(conn)

    def clean(self):
        """
        Drop and recreate the graph.

        - Drops the graph if it exists
        - Calls setup() to recreate an empty, indexed graph
        """
        logger.info("Cleaning graph (drop & recreate)...")
        conn = self._get_connection()

        try:
            # Ensure AGE is loaded (drop_graph lives in the extension)
            self._ensure_age_loaded(conn)

            with conn.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    cursor.execute("SELECT drop_graph(%s, true);", (self.graph_name,))
                    logger.info(f"Dropped graph: {self.graph_name}")
                else:
                    logger.info(f"Graph {self.graph_name} does not exist; nothing to drop")

        except Exception as e:
            logger.error(f"Clean failed: {e}", exc_info=True)
            raise
        finally:
            self._return_connection(conn)

        # Recreate schema + indexes
        self.setup()

    # -------------------------------------------------------------------------
    # Utility to build Cypher list literals for AGE
    # -------------------------------------------------------------------------

    def _to_cypher_list(self, data: List[Dict]) -> str:
        """
        Convert a list of dicts to Cypher-compatible list string.

        AGE requires map keys to be unquoted, so we:
        - JSON-encode the list
        - remove quotes around keys
        - escape single quotes for embedding in SQL
        """
        json_str = json.dumps(data)
        cypher_str = re.sub(r'"(\w+)":', r'\1:', json_str)
        return cypher_str.replace("'", "''")

    # -------------------------------------------------------------------------
    # Ingestion
    # -------------------------------------------------------------------------

    def ingest_batch(self, events_batch: List[Dict[str, Any]]):
        """
        Ingest a batch of events with optimized processing.

        Args:
            events_batch: List of event dictionaries
        """
        if not events_batch:
            return

        batch_size = len(events_batch)
        logger.info(f"Ingesting batch of {batch_size} events...")
        start_time = time.time()

        conn = self._get_connection()
        try:
            # Ensure AGE loaded for this connection
            self._ensure_age_loaded(conn)

            # Prepare batch data
            batch_data = []
            now_iso = datetime.now().isoformat()
            for ev in events_batch:
                batch_data.append({
                    'id': ev['id'],
                    'status': 'NEW',
                    'created_at': now_iso,
                    'payload': json.dumps(ev.get('payload', {})),
                    'correlation_ids': ev['correlation_ids']
                })

            batch_cypher = self._to_cypher_list(batch_data)

            # Query 1: Create Events
            query_create = f"""
                UNWIND {batch_cypher} as row
                CREATE (:Event {{
                    id: row.id,
                    status: row.status,
                    created_at: row.created_at,
                    payload: row.payload
                }})
            """
            self._execute_cypher(query_create, conn=conn)
            logger.debug(f"Created {batch_size} Event nodes")

            # Query 2: Link Correlations with MERGE for race condition safety
            query_link = f"""
                UNWIND {batch_cypher} as row
                MATCH (e:Event {{id: row.id}})
                UNWIND row.correlation_ids as cid
                MERGE (c:Correlation {{id: cid}})
                MERGE (e)-[:HAS_KEY]->(c)
            """
            self._execute_cypher(query_link, conn=conn)
            logger.debug(f"Linked correlations for {batch_size} events")

            elapsed = time.time() - start_time
            throughput = batch_size / elapsed if elapsed > 0 else 0.0
            logger.info(
                f"Batch ingestion complete: {batch_size} events in {elapsed:.2f}s "
                f"({throughput:.0f} events/sec)"
            )

        except Exception as e:
            logger.error(f"Batch ingestion failed: {e}", exc_info=True)
            raise
        finally:
            self._return_connection(conn)

    # -------------------------------------------------------------------------
    # Batch processing / journey stitching
    # -------------------------------------------------------------------------

    def _process_batch_worker(
        self,
        events: List[Dict],
        all_cids: set,
        cid_to_journeys: Dict[str, List[str]],
        existing_journeys_map: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Worker function that computes connectivity and required actions
        (new journeys, merges, cid_links) for a batch of events.

        Returns:
            Dictionary with:
              - new_journeys: List[{'id', 'created_at'}]
              - merges: List[{'winner', 'loser'}]
              - cid_links: List[{'jid', 'cid'}]
              - event_ids: List[event_id]
        """
        parent: Dict[str, str] = {}

        def find(i: str) -> str:
            if i not in parent:
                parent[i] = i
            if parent[i] != i:
                parent[i] = find(parent[i])
            return parent[i]

        def union(i: str, j: str):
            root_i = find(i)
            root_j = find(j)
            if root_i != root_j:
                parent[root_i] = root_j

        # Union CIDs within each event and link CIDs to existing journeys
        for ev in events:
            cids = ev['cids']
            if not cids:
                continue

            first = cids[0]
            for other in cids[1:]:
                union(first, other)

            for cid in cids:
                if cid in cid_to_journeys:
                    for jid in cid_to_journeys[cid]:
                        union(cid, jid)

        # Build groups (connected components)
        groups: Dict[str, Dict[str, set]] = defaultdict(lambda: {'cids': set(), 'jids': set()})

        for cid in all_cids:
            root = find(cid)
            groups[root]['cids'].add(cid)

        for jid in existing_journeys_map.keys():
            if jid in parent:
                root = find(jid)
                groups[root]['jids'].add(jid)

        new_journeys: List[Dict[str, str]] = []
        merges: List[Dict[str, str]] = []
        cid_links: List[Dict[str, str]] = []

        for root, group in groups.items():
            jids = list(group['jids'])
            target_jid: Optional[str] = None

            if not jids:
                # Create new journey
                target_jid = f"journey_{uuid.uuid4()}"
                created_at = datetime.now().isoformat()
                new_journeys.append({'id': target_jid, 'created_at': created_at})
            elif len(jids) == 1:
                target_jid = jids[0]
            else:
                # Merge journeys; keep the oldest
                jids.sort(key=lambda x: existing_journeys_map[x])
                target_jid = jids[0]
                losers = jids[1:]
                for loser in losers:
                    merges.append({'winner': target_jid, 'loser': loser})

            for cid in group['cids']:
                cid_links.append({'jid': target_jid, 'cid': cid})

        return {
            'new_journeys': new_journeys,
            'merges': merges,
            'cid_links': cid_links,
            'event_ids': [e['id'] for e in events]
        }

    def process_events(self):
        """
        Process events with batched processing and race condition handling.
        """
        logger.info("Starting event processing...")
        total_processed = 0
        batch_count = 0
        start_time = time.time()

        BATCH_SIZE = 5000  # batch size for NEW events

        while True:
            batch_start = time.time()
            conn = self._get_connection()

            try:
                self._ensure_age_loaded(conn)

                # Fetch NEW events
                # NOTE: MATCH (e:Event {status: 'NEW'}) form uses properties @> pattern
                fetch_query = f"""
                    MATCH (e:Event {{status: 'NEW'}})
                    WITH e LIMIT {BATCH_SIZE}
                    MATCH (e)-[:HAS_KEY]->(c:Correlation)
                    RETURN e.id, collect(c.id)
                """
                rows = self._execute_cypher(
                    fetch_query,
                    cols="id agtype, cids agtype",
                    conn=conn
                )

                if not rows:
                    logger.info("No more NEW events to process")
                    break

                events = []
                all_cids: set = set()
                for row in rows:
                    eid = json.loads(row[0])
                    cids = json.loads(row[1])
                    events.append({'id': eid, 'cids': cids})
                    all_cids.update(cids)

                if not all_cids:
                    logger.info("Batch had no correlations; marking events as PROCESSED")
                    event_ids = [{'id': e['id']} for e in events]
                    batch_cypher = self._to_cypher_list(event_ids)
                    mark_query = f"""
                        UNWIND {batch_cypher} as row
                        MATCH (e:Event {{id: row.id}})
                        SET e.status = 'PROCESSED'
                    """
                    self._execute_cypher(mark_query, conn=conn)
                    continue

                logger.info(
                    f"Processing batch {batch_count + 1}: "
                    f"{len(events)} events, {len(all_cids)} unique correlations"
                )

                # Bulk lookup existing journeys for these CIDs
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
                    conn=conn
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

                logger.debug(f"Found {len(existing_journeys_map)} existing journeys in this batch")

                # Compute actions (union-find in memory)
                actions = self._process_batch_worker(
                    events,
                    all_cids,
                    cid_to_journeys,
                    existing_journeys_map
                )

                logger.info(
                    f"Batch actions: {len(actions['new_journeys'])} new journeys, "
                    f"{len(actions['merges'])} merges, {len(actions['cid_links'])} links"
                )

                # A. Create new journeys
                if actions['new_journeys']:
                    batch_cypher = self._to_cypher_list(actions['new_journeys'])
                    query = f"""
                        UNWIND {batch_cypher} as row
                        CREATE (j:Journey {{id: row.id, created_at: row.created_at}})
                    """
                    self._execute_cypher(query, conn=conn)
                    logger.debug(f"Created {len(actions['new_journeys'])} new journeys")

                # B. Execute merges (separate queries for robustness)
                if actions['merges']:
                    for merge in actions['merges']:
                        try:
                            merge_query = f"""
                                MATCH (loser:Journey {{id: '{merge['loser']}'}})
                                MATCH (winner:Journey {{id: '{merge['winner']}'}})
                                OPTIONAL MATCH (c:Correlation)-[r:PART_OF]->(loser)
                                DELETE r
                                WITH c, winner
                                WHERE c IS NOT NULL
                                MERGE (c)-[:PART_OF]->(winner)
                            """
                            self._execute_cypher(merge_query, conn=conn)

                            delete_query = (
                                f"MATCH (j:Journey {{id: '{merge['loser']}'}}) DELETE j"
                            )
                            self._execute_cypher(delete_query, conn=conn)

                        except Exception as e:
                            logger.warning(f"Merge conflict handled: {e}")

                    logger.debug(f"Executed {len(actions['merges'])} journey merges")

                # C. Link CIDs to journeys
                if actions['cid_links']:
                    batch_cypher = self._to_cypher_list(actions['cid_links'])
                    query = f"""
                        UNWIND {batch_cypher} as row
                        MATCH (j:Journey {{id: row.jid}})
                        MERGE (c:Correlation {{id: row.cid}})
                        MERGE (c)-[:PART_OF]->(j)
                    """
                    self._execute_cypher(query, conn=conn)
                    logger.debug(f"Linked {len(actions['cid_links'])} correlations to journeys")

                # D. Mark Events as PROCESSED
                event_ids = [{'id': eid} for eid in actions['event_ids']]
                batch_cypher = self._to_cypher_list(event_ids)
                mark_query = f"""
                    UNWIND {batch_cypher} as row
                    MATCH (e:Event {{id: row.id}})
                    SET e.status = 'PROCESSED'
                """
                self._execute_cypher(mark_query, conn=conn)

                total_processed += len(events)
                batch_count += 1

                batch_elapsed = time.time() - batch_start
                batch_throughput = len(events) / batch_elapsed if batch_elapsed > 0 else 0.0

                logger.info(
                    f"Batch {batch_count} complete: {len(events)} events in {batch_elapsed:.2f}s "
                    f"({batch_throughput:.0f} events/sec)"
                )

            except Exception as e:
                logger.error(f"Batch processing failed: {e}", exc_info=True)
                raise
            finally:
                self._return_connection(conn)

        total_elapsed = time.time() - start_time
        overall_throughput = total_processed / total_elapsed if total_elapsed > 0 else 0.0

        logger.info(
            f"Event processing complete: {total_processed} events in {total_elapsed:.2f}s "
            f"({overall_throughput:.0f} events/sec, {batch_count} batches)"
        )

    # -------------------------------------------------------------------------
    # Querying journeys
    # -------------------------------------------------------------------------

    def get_journey(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve journey information for a given event.

        Args:
            event_id: Event identifier

        Returns:
            Dictionary with journey_id and events list, or None if not found
        """
        query = f"""
            MATCH (start_e:Event {{id: '{event_id}'}})-[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j:Journey)
            MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(all_e:Event)
            RETURN j.id, collect(DISTINCT all_e.id)
        """

        conn = self._get_connection()
        try:
            self._ensure_age_loaded(conn)
            rows = self._execute_cypher(
                query,
                cols="jid agtype, events agtype",
                conn=conn
            )
            if rows:
                return {
                    "journey_id": json.loads(rows[0][0]),
                    "events": json.loads(rows[0][1])
                }
            return None
        finally:
            self._return_connection(conn)

    # -------------------------------------------------------------------------
    # Shutdown
    # -------------------------------------------------------------------------

    def close(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Connection pool closed")


# -----------------------------------------------------------------------------
# Benchmark entry-point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching

    DB_CONFIG = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": 5436,
    }

    # Initialize with connection pooling
    jm = ApacheAgeJourneyManager(DB_CONFIG, pool_size=10, max_workers=4)

    # Wait for DB to be ready
    logger.info("Waiting for database connection...")
    for i in range(30):
        try:
            conn = jm._get_connection()
            jm._return_connection(conn)
            logger.info("Database connection established")
            break
        except Exception:
            time.sleep(2)
    else:
        logger.error("Could not establish database connection; exiting")
        sys.exit(1)

    # Initial setup + clean to ensure a fresh graph
    jm.setup()
    jm.clean()  # clean() will drop if exists and recreate an empty, indexed graph

    # -------------------------------------------------------------------------
    # Benchmark 1: larger event volume per app (optional)
    # -------------------------------------------------------------------------
    NUM_JOURNEYS = 1000
    EVENTS_PER_APP = 3
    NUM_APPS = 5

    logger.info("Starting Benchmark 1: Apache AGE (Production Optimized)")
    logger.info(
        f"Configuration: {NUM_JOURNEYS} journeys, {NUM_APPS} apps, "
        f"{EVENTS_PER_APP} events/app"
    )

    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)

    # Clean between benchmark runs to avoid mixing data
    # jm.clean()

    # # -------------------------------------------------------------------------
    # # Benchmark 2: main config (5 events per app)
    # # -------------------------------------------------------------------------
    # NUM_JOURNEYS = 1000
    # EVENTS_PER_APP = 5
    # NUM_APPS = 5

    # logger.info("Starting Benchmark 2: Apache AGE (Production Optimized)")
    # logger.info(
    #     f"Configuration: {NUM_JOURNEYS} journeys, {NUM_APPS} apps, "
    #     f"{EVENTS_PER_APP} events/app"
    # )

    # generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)

    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process

    total_events = NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP
    total_time = ingest_time + process_time

    logger.info("=" * 60)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total Events: {total_events}")
    logger.info(f"Ingestion Time: {ingest_time:.2f}s ({total_events / ingest_time:.0f} events/sec)")
    logger.info(f"Processing Time: {process_time:.2f}s ({total_events / process_time:.0f} events/sec)")
    logger.info(f"Total Time: {total_time:.2f}s ({total_events / total_time:.0f} events/sec)")
    logger.info("=" * 60)

    results = {
        "total_events": total_events,
        "ingest_time": ingest_time,
        "process_time": process_time,
        "total_time": total_time,
        "ingest_throughput": total_events / ingest_time,
        "process_throughput": total_events / process_time,
        "overall_throughput": total_events / total_time,
    }

    with open("age_results.json", "w") as f:
        json.dump(results, f, indent=2)

    logger.info("Validating journey stitching...")
    validate_stitching(jm, generated_data)

    jm.close()
    logger.info("Benchmark complete")
