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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

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
    - Parallel processing for improved throughput
    - Race condition handling for multi-pod deployments
    - Comprehensive logging for observability
    """
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 10, max_workers: int = 4):
        """
        Initialize the Journey Manager with connection pooling.
        
        Args:
            db_config: Database connection configuration
            pool_size: Number of connections in the pool
            max_workers: Number of parallel workers for processing
        """
        self.db_config = db_config
        self.graph_name = "benchmark_graph"
        self.pool_size = pool_size
        self.max_workers = max_workers
        self.connection_pool = None
        self._lock = threading.Lock()
        
        logger.info(f"Initializing ApacheAgeJourneyManager with pool_size={pool_size}, max_workers={max_workers}")
        self._init_connection_pool()

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

    def _get_connection(self):
        """Get a connection from the pool."""
        try:
            conn = self.connection_pool.getconn()
            conn.autocommit = True
            return conn
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}", exc_info=True)
            raise

    def _return_connection(self, conn):
        """Return a connection to the pool."""
        try:
            self.connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to return connection to pool: {e}", exc_info=True)

    def _execute_cypher(self, query: str, params: Optional[Dict] = None, cols: str = "v agtype", 
                       conn=None, retry_count: int = 3) -> List[Tuple]:
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
        should_return_conn = False
        if conn is None:
            conn = self._get_connection()
            should_return_conn = True
            
        attempt = 0
        last_error = None
        
        while attempt < retry_count:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("LOAD 'age';")
                    cursor.execute("SET search_path = ag_catalog, '$user', public;")
                    
                    full_query = f"SELECT * FROM cypher('{self.graph_name}', $$ {query} $$) as ({cols});"
                    
                    start_time = time.time()
                    cursor.execute(full_query)
                    
                    try:
                        results = cursor.fetchall()
                        elapsed = time.time() - start_time
                        
                        if elapsed > 1.0:  # Log slow queries
                            logger.warning(f"Slow query detected ({elapsed:.2f}s): {query[:100]}...")
                        
                        return results
                    except psycopg2.ProgrammingError:
                        return []
                        
            except psycopg2.OperationalError as e:
                last_error = e
                attempt += 1
                logger.warning(f"Query failed (attempt {attempt}/{retry_count}): {e}")
                
                if attempt < retry_count:
                    time.sleep(0.1 * attempt)  # Exponential backoff
                    
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}", exc_info=True)
                raise
            finally:
                if should_return_conn and attempt >= retry_count:
                    self._return_connection(conn)
                    
        if should_return_conn:
            self._return_connection(conn)
            
        logger.error(f"Query failed after {retry_count} attempts: {last_error}")
        raise last_error

    def setup(self):
        """Initialize the graph schema with optimized indices."""
        logger.info("Setting up graph schema and indices...")
        conn = self._get_connection()
        
        try:
            with conn.cursor() as cursor:
                # Create AGE extension
                cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
                cursor.execute("LOAD 'age';")
                cursor.execute("SET search_path = ag_catalog, '$user', public;")
                
                # Create graph if not exists
                cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute(f"SELECT create_graph('{self.graph_name}');")
                    logger.info(f"Created graph: {self.graph_name}")
                
                # Create node labels
                self._execute_cypher("CREATE (:Event), (:Correlation), (:Journey)", conn=conn)
                logger.info("Created node labels: Event, Correlation, Journey")
                
                # Create optimized indices
                indices_created = 0
                for label in ["Event", "Correlation", "Journey"]:
                    try:
                        # GIN index on properties for JSON lookups
                        index_query = f'CREATE INDEX IF NOT EXISTS "idx_{label}_properties" ON "{self.graph_name}"."{label}" USING GIN (properties);'
                        cursor.execute(index_query)
                        indices_created += 1
                        logger.info(f"Created GIN index for {label}")
                        
                        # B-tree index on id field (extracted from properties)
                        # This requires creating an expression index
                        id_index_query = f'CREATE INDEX IF NOT EXISTS "idx_{label}_id" ON "{self.graph_name}"."{label}" ((properties->>\'id\'));'
                        cursor.execute(id_index_query)
                        indices_created += 1
                        logger.info(f"Created B-tree index on id for {label}")
                        
                    except Exception as e:
                        logger.warning(f"Could not create index for {label}: {e}")
                        conn.rollback()
                
                logger.info(f"Setup complete. Created {indices_created} indices.")
                
        except Exception as e:
            logger.error(f"Setup failed: {e}", exc_info=True)
            raise
        finally:
            self._return_connection(conn)

    def clean(self):
        """Drop and recreate the graph."""
        logger.info("Cleaning graph...")
        conn = self._get_connection()
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("LOAD 'age';")
                cursor.execute("SET search_path = ag_catalog, '$user', public;")
                cursor.execute(f"SELECT drop_graph('{self.graph_name}', true);")
                logger.info(f"Dropped graph: {self.graph_name}")
        except Exception as e:
            logger.error(f"Clean failed: {e}", exc_info=True)
            raise
        finally:
            self._return_connection(conn)
            
        self.setup()

    def _to_cypher_list(self, data: List[Dict]) -> str:
        """
        Convert a list of dicts to Cypher-compatible list string.
        AGE requires map keys to be unquoted.
        """
        json_str = json.dumps(data)
        cypher_str = re.sub(r'"(\w+)":', r'\1:', json_str)
        return cypher_str.replace("'", "''")

    def ingest_batch(self, events_batch: List[Dict[str, Any]]):
        """
        Ingest a batch of events with optimized parallel processing.
        
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
            # Prepare batch data
            batch_data = []
            for ev in events_batch:
                batch_data.append({
                    'id': ev['id'],
                    'status': 'NEW',
                    'created_at': datetime.now().isoformat(),
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
            throughput = batch_size / elapsed if elapsed > 0 else 0
            logger.info(f"Batch ingestion complete: {batch_size} events in {elapsed:.2f}s ({throughput:.0f} events/sec)")
            
        except Exception as e:
            logger.error(f"Batch ingestion failed: {e}", exc_info=True)
            raise
        finally:
            self._return_connection(conn)

    def _process_batch_worker(self, events: List[Dict], all_cids: set, 
                             cid_to_journeys: Dict, existing_journeys_map: Dict) -> Dict:
        """
        Worker function for parallel batch processing.
        
        Returns:
            Dictionary with actions to perform (new_journeys, merges, cid_links, event_ids)
        """
        # Union-Find for grouping connected components
        parent = {}
        
        def find(i):
            if i not in parent:
                parent[i] = i
            if parent[i] != i:
                parent[i] = find(parent[i])
            return parent[i]
        
        def union(i, j):
            root_i = find(i)
            root_j = find(j)
            if root_i != root_j:
                parent[root_i] = root_j
        
        # Union cids within each event
        for ev in events:
            cids = ev['cids']
            if not cids:
                continue
            first = cids[0]
            for other in cids[1:]:
                union(first, other)
                
            # Union cids with their existing journeys
            for cid in cids:
                if cid in cid_to_journeys:
                    for jid in cid_to_journeys[cid]:
                        union(cid, jid)
        
        # Resolve components
        groups = defaultdict(lambda: {'cids': set(), 'jids': set()})
        
        for cid in all_cids:
            root = find(cid)
            groups[root]['cids'].add(cid)
        
        for jid in existing_journeys_map.keys():
            if jid in parent:
                root = find(jid)
                groups[root]['jids'].add(jid)
        
        # Prepare actions
        new_journeys = []
        merges = []
        cid_links = []
        
        for root, group in groups.items():
            jids = list(group['jids'])
            target_jid = None
            
            if not jids:
                # Create new journey
                target_jid = f"journey_{uuid.uuid4()}"
                created_at = datetime.now().isoformat()
                new_journeys.append({'id': target_jid, 'created_at': created_at})
            elif len(jids) == 1:
                target_jid = jids[0]
            else:
                # Merge - keep oldest
                jids.sort(key=lambda x: existing_journeys_map[x])
                target_jid = jids[0]
                losers = jids[1:]
                for loser in losers:
                    merges.append({'winner': target_jid, 'loser': loser})
            
            # Link all CIDs to target journey
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
        Process events with parallel batch processing and race condition handling.
        """
        logger.info("Starting event processing...")
        total_processed = 0
        batch_count = 0
        start_time = time.time()
        
        BATCH_SIZE = 5000  # Increased batch size for better throughput
        
        while True:
            batch_start = time.time()
            
            # Fetch NEW events
            conn = self._get_connection()
            try:
                fetch_query = f"""
                    MATCH (e:Event {{status: 'NEW'}})
                    WITH e LIMIT {BATCH_SIZE}
                    MATCH (e)-[:HAS_KEY]->(c:Correlation)
                    RETURN e.id, collect(c.id)
                """
                
                rows = self._execute_cypher(fetch_query, cols="id agtype, cids agtype", conn=conn)
                
                if not rows:
                    logger.info("No more NEW events to process")
                    break
                
                # Parse rows
                events = []
                all_cids = set()
                for row in rows:
                    eid = json.loads(row[0])
                    cids = json.loads(row[1])
                    events.append({'id': eid, 'cids': cids})
                    all_cids.update(cids)
                
                if not all_cids:
                    continue
                
                logger.info(f"Processing batch {batch_count + 1}: {len(events)} events, {len(all_cids)} unique correlations")
                
                # Bulk lookup existing journeys
                all_cids_list = list(all_cids)
                cids_json = json.dumps(all_cids_list).replace("'", "''")
                
                find_journey_query = f"""
                    MATCH (c:Correlation)-[:PART_OF]->(j:Journey)
                    WHERE c.id IN {cids_json}
                    RETURN c.id, j.id, j.created_at
                """
                
                j_rows = self._execute_cypher(find_journey_query, cols="cid agtype, jid agtype, created_at agtype", conn=conn)
                
                cid_to_journeys = defaultdict(list)
                existing_journeys_map = {}
                
                if j_rows:
                    for row in j_rows:
                        cid = json.loads(row[0])
                        jid = json.loads(row[1])
                        created_at = json.loads(row[2])
                        cid_to_journeys[cid].append(jid)
                        existing_journeys_map[jid] = created_at
                
                logger.debug(f"Found {len(existing_journeys_map)} existing journeys")
                
                # Process batch (compute actions)
                actions = self._process_batch_worker(events, all_cids, cid_to_journeys, existing_journeys_map)
                
                logger.info(f"Batch actions: {len(actions['new_journeys'])} new journeys, "
                          f"{len(actions['merges'])} merges, {len(actions['cid_links'])} links")
                
                # Execute actions with race condition handling
                
                # A. Create New Journeys (use CREATE since we know IDs are unique)
                if actions['new_journeys']:
                    batch_cypher = self._to_cypher_list(actions['new_journeys'])
                    query = f"""
                        UNWIND {batch_cypher} as row
                        CREATE (j:Journey {{id: row.id, created_at: row.created_at}})
                    """
                    self._execute_cypher(query, conn=conn)
                    logger.debug(f"Created {len(actions['new_journeys'])} new journeys")
                
                # B. Execute Merges (with conflict handling)
                if actions['merges']:
                    for merge in actions['merges']:
                        try:
                            # Use individual queries for merges to handle conflicts
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
                            
                            # Delete loser journey
                            delete_query = f"MATCH (j:Journey {{id: '{merge['loser']}'}}) DELETE j"
                            self._execute_cypher(delete_query, conn=conn)
                        except Exception as e:
                            logger.warning(f"Merge conflict handled: {e}")
                    
                    logger.debug(f"Executed {len(actions['merges'])} merges")
                
                # C. Link CIDs to Journeys
                if actions['cid_links']:
                    batch_cypher = self._to_cypher_list(actions['cid_links'])
                    query = f"""
                        UNWIND {batch_cypher} as row
                        MATCH (j:Journey {{id: row.jid}})
                        MERGE (c:Correlation {{id: row.cid}})
                        MERGE (c)-[:PART_OF]->(j)
                    """
                    self._execute_cypher(query, conn=conn)
                    logger.debug(f"Linked {len(actions['cid_links'])} correlations")
                
                # D. Mark Events as PROCESSED
                event_ids = [{'id': eid} for eid in actions['event_ids']]
                batch_cypher = self._to_cypher_list(event_ids)
                query = f"""
                    UNWIND {batch_cypher} as row
                    MATCH (e:Event {{id: row.id}})
                    SET e.status = 'PROCESSED'
                """
                self._execute_cypher(query, conn=conn)
                
                total_processed += len(events)
                batch_count += 1
                
                batch_elapsed = time.time() - batch_start
                batch_throughput = len(events) / batch_elapsed if batch_elapsed > 0 else 0
                
                logger.info(f"Batch {batch_count} complete: {len(events)} events in {batch_elapsed:.2f}s "
                          f"({batch_throughput:.0f} events/sec)")
                
            except Exception as e:
                logger.error(f"Batch processing failed: {e}", exc_info=True)
                raise
            finally:
                self._return_connection(conn)
        
        total_elapsed = time.time() - start_time
        overall_throughput = total_processed / total_elapsed if total_elapsed > 0 else 0
        
        logger.info(f"Event processing complete: {total_processed} events in {total_elapsed:.2f}s "
                   f"({overall_throughput:.0f} events/sec, {batch_count} batches)")

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
            rows = self._execute_cypher(query, cols="jid agtype, events agtype", conn=conn)
            if rows:
                return {
                    "journey_id": json.loads(rows[0][0]),
                    "events": json.loads(rows[0][1])
                }
            return None
        finally:
            self._return_connection(conn)

    def close(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Connection pool closed")


if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching
    
    DB_CONFIG = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": 5436
    }
    
    # Initialize with connection pooling
    jm = ApacheAgeJourneyManager(DB_CONFIG, pool_size=10, max_workers=4)
    
    # Wait for DB
    logger.info("Waiting for database connection...")
    for i in range(30):
        try:
            conn = jm._get_connection()
            jm._return_connection(conn)
            logger.info("Database connection established")
            break
        except:
            time.sleep(2)
    
    jm.setup()
    jm.clean()
    
    NUM_JOURNEYS = 1000
    EVENTS_PER_APP = 30
    NUM_APPS = 5
    
    logger.info(f"Starting Benchmark: Apache AGE (Production Optimized)")
    logger.info(f"Configuration: {NUM_JOURNEYS} journeys, {NUM_APPS} apps, {EVENTS_PER_APP} events/app")
    
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)
    
    jm.setup()
    jm.clean()
    
    NUM_JOURNEYS = 1000
    EVENTS_PER_APP = 5
    NUM_APPS = 5
    
    logger.info(f"Starting Benchmark: Apache AGE (Production Optimized)")
    logger.info(f"Configuration: {NUM_JOURNEYS} journeys, {NUM_APPS} apps, {EVENTS_PER_APP} events/app")
    
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    total_events = NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP
    total_time = ingest_time + process_time
    
    logger.info(f"=" * 60)
    logger.info(f"BENCHMARK RESULTS")
    logger.info(f"=" * 60)
    logger.info(f"Total Events: {total_events}")
    logger.info(f"Ingestion Time: {ingest_time:.2f}s ({total_events/ingest_time:.0f} events/sec)")
    logger.info(f"Processing Time: {process_time:.2f}s ({total_events/process_time:.0f} events/sec)")
    logger.info(f"Total Time: {total_time:.2f}s ({total_events/total_time:.0f} events/sec)")
    logger.info(f"=" * 60)
    
    results = {
        "total_events": total_events,
        "ingest_time": ingest_time,
        "process_time": process_time,
        "total_time": total_time,
        "ingest_throughput": total_events / ingest_time,
        "process_throughput": total_events / process_time,
        "overall_throughput": total_events / total_time
    }
    
    with open("age_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info("Validating journey stitching...")
    validate_stitching(jm, generated_data)
    
    jm.close()
    logger.info("Benchmark complete")
