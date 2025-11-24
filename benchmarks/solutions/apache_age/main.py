import psycopg2
from psycopg2 import pool, extras
import json
import time
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
logger = logging.getLogger('ApacheAGE_Optimized')


class ApacheAgeJourneyManager(JourneyManager):
    """
    Optimized Apache AGE Manager using Named Parameters ($batch) and GIN Indexes.
    """

    def __init__(self, db_config: Dict[str, Any], pool_size: int = 10, max_workers: int = 4):
        self.db_config = db_config
        self.graph_name = "benchmark_graph"
        self.pool_size = pool_size
        self._lock = threading.Lock()
        self._initialized_connections: Set[int] = set()

        # OPTIMIZATION: search_path and synchronous_commit=off
        self.db_config['options'] = f"-c search_path=ag_catalog,public -c synchronous_commit=off"

        logger.info(f"Initializing Optimized Manager (Pool: {pool_size})")
        self._init_connection_pool()

    def _init_connection_pool(self):
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                minconn=5,
                maxconn=self.pool_size,
                **self.db_config
            )
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def _get_connection(self) -> psycopg2.extensions.connection:
        conn = self.connection_pool.getconn()
        conn.autocommit = True
        self._ensure_age_loaded(conn)
        return conn

    def _return_connection(self, conn):
        if conn:
            self.connection_pool.putconn(conn)

    def _ensure_age_loaded(self, conn):
        """Load AGE extension for the session if not already loaded."""
        conn_id = id(conn)
        if conn_id in self._initialized_connections:
            return
        try:
            with conn.cursor() as cursor:
                cursor.execute("LOAD 'age';")
                cursor.execute("SET search_path = ag_catalog, '$user', public;")
            self._initialized_connections.add(conn_id)
        except Exception as e:
            logger.error(f"Failed to LOAD 'age': {e}")
            raise

    # -------------------------------------------------------------------------
    # OPTIMIZATION: Named Parameter Execution
    # -------------------------------------------------------------------------

    def _exec_parameterized_cypher(self, cursor, cypher_logic: str, param_data: Any, cols: str = "a agtype"):
        """
        Executes a Cypher query using proper AGE parameterization.
        Wraps data in {'batch': param_data} to satisfy AGE's map requirement.
        """
        # Wrap data in a map (AGE requires the top-level param to be a map)
        wrapper = {"batch": param_data}
        wrapper_json = json.dumps(wrapper)

        stmt_name = f"stmt_{uuid.uuid4().hex}"
        
        # 1. Prepare
        prepare_sql = f"""
            PREPARE {stmt_name} (agtype) AS 
            SELECT * FROM cypher('{self.graph_name}', $$
                {cypher_logic}
            $$, $1) as ({cols});
        """
        cursor.execute(prepare_sql)

        # 2. Execute
        execute_sql = f"EXECUTE {stmt_name} (%s::agtype)"
        cursor.execute(execute_sql, (wrapper_json,))

        # 3. Deallocate
        cursor.execute(f"DEALLOCATE {stmt_name}")

    # -------------------------------------------------------------------------
    # Setup
    # -------------------------------------------------------------------------

    def setup(self):
        logger.info("Setting up Graph Schema & GIN Indexes...")
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
                cursor.execute("LOAD 'age';")
                
                cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("SELECT create_graph(%s);", (self.graph_name,))
                
                for label in ["Event", "Correlation", "Journey"]:
                    cursor.execute(f"SELECT create_vlabel('{self.graph_name}', '{label}');")
                for label in ["HAS_KEY", "PART_OF"]:
                    cursor.execute(f"SELECT create_elabel('{self.graph_name}', '{label}');")

                # GIN Indexes for fast @> jsonb operations
                for label in ["Event", "Correlation", "Journey"]:
                    cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS "idx_{label}_gin" 
                        ON "{self.graph_name}"."{label}" 
                        USING GIN (properties);
                    """)
        finally:
            self._return_connection(conn)

    def clean(self):
        logger.info("Cleaning graph...")
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
                if cursor.fetchone()[0] > 0:
                    cursor.execute("SELECT drop_graph(%s, true);", (self.graph_name,))
        except Exception:
            pass 
        finally:
            self._return_connection(conn)
        self.setup()

    # -------------------------------------------------------------------------
    # Ingestion - STABILIZED (Split Query Strategy)
    # -------------------------------------------------------------------------

    def ingest_batch(self, events_batch: List[Dict[str, Any]]):
        if not events_batch: return

        conn = self._get_connection()
        try:
            now_iso = datetime.now().isoformat()
            
            # Phase 1: Prepare Event Data
            # We separate this list to avoid complex nested logic in the DB
            event_rows = []
            link_rows = []

            for ev in events_batch:
                # Data for Event Creation
                event_rows.append({
                    'id': ev['id'],
                    'status': 'NEW',
                    'created_at': now_iso,
                    'payload': json.dumps(ev.get('payload', {}))
                })
                
                # Data for Linkage (Only if correlations exist)
                # Optimization: Don't send payload here, just ID and CIDs
                if ev['correlation_ids']:
                    link_rows.append({
                        'id': ev['id'],
                        'correlations': ev['correlation_ids']
                    })
            
            # QUERY 1: Create Events (Fast, no locking issues)
            # Use $batch variable which maps to event_rows
            q_create = """
                UNWIND $batch as row
                CREATE (:Event {
                    id: row.id, 
                    status: row.status, 
                    created_at: row.created_at, 
                    payload: row.payload
                })
            """
            
            # QUERY 2: Link Correlations
            # Matches the just-created events and merges correlations
            q_link = """
                UNWIND $batch as row
                MATCH (e:Event {id: row.id})
                UNWIND row.correlations as cid
                MERGE (c:Correlation {id: cid})
                MERGE (e)-[:HAS_KEY]->(c)
            """
            
            with conn.cursor() as cursor:
                # Execute Split Queries
                if event_rows:
                    self._exec_parameterized_cypher(cursor, q_create, event_rows)
                
                if link_rows:
                    self._exec_parameterized_cypher(cursor, q_link, link_rows)
                
        except Exception as e:
            logger.error(f"Ingest failed: {e}")
            raise
        finally:
            self._return_connection(conn)

    # -------------------------------------------------------------------------
    # Processing
    # -------------------------------------------------------------------------

    def process_events(self):
        BATCH_SIZE = 5000 
        logger.info("Starting processing loop...")
        
        while True:
            start_time = time.time()
            conn = self._get_connection()
            
            try:
                # 1. Fetch NEW events
                # Matches via GIN Index
                fetch_query = f"""
                    SELECT * FROM cypher('{self.graph_name}', $$
                        MATCH (e:Event {{status: 'NEW'}})
                        WITH e LIMIT {BATCH_SIZE}
                        MATCH (e)-[:HAS_KEY]->(c:Correlation)
                        RETURN e.id, collect(c.id)
                    $$) as (id agtype, cids agtype);
                """
                
                with conn.cursor() as cursor:
                    cursor.execute(fetch_query)
                    rows = cursor.fetchall()
                
                if not rows:
                    break

                events = []
                all_cids = set()
                
                for r in rows:
                    eid = r[0] 
                    cids = r[1] 
                    if isinstance(eid, str): eid = json.loads(eid)
                    if isinstance(cids, str): cids = json.loads(cids)
                    events.append({'id': eid, 'cids': cids})
                    all_cids.update(cids)

                logger.info(f"Processing batch: {len(events)} events, {len(all_cids)} correlations")

                # 2. Bulk Lookup Existing Journeys
                cid_list = list(all_cids)
                cid_wrapper = {"batch": cid_list} 
                cid_json = json.dumps(cid_wrapper)
                
                lookup_logic = """
                    WITH $batch as target_cids
                    MATCH (c:Correlation)-[:PART_OF]->(j:Journey)
                    WHERE c.id IN target_cids
                    RETURN c.id, j.id, j.created_at
                """
                
                cid_to_journeys = defaultdict(list)
                existing_journeys_map = {}
                
                with conn.cursor() as cursor:
                    stmt_name = f"stmt_lookup_{uuid.uuid4().hex}"
                    cursor.execute(f"""
                        PREPARE {stmt_name} (agtype) AS 
                        SELECT * FROM cypher('{self.graph_name}', $${lookup_logic}$$, $1) 
                        as (cid agtype, jid agtype, created_at agtype);
                    """)
                    cursor.execute(f"EXECUTE {stmt_name} (%s::agtype)", (cid_json,))
                    j_rows = cursor.fetchall()
                    cursor.execute(f"DEALLOCATE {stmt_name}")

                for r in j_rows:
                    cid = json.loads(r[0]) if isinstance(r[0], str) else r[0]
                    jid = json.loads(r[1]) if isinstance(r[1], str) else r[1]
                    cat = json.loads(r[2]) if isinstance(r[2], str) else r[2]
                    
                    cid_to_journeys[cid].append(jid)
                    existing_journeys_map[jid] = cat

                # 3. In-Memory Stitching
                actions = self._compute_graph_actions(events, all_cids, cid_to_journeys, existing_journeys_map)

                # 4. Execute Writes using $batch variable
                with conn.cursor() as cursor:
                    
                    # A. Create New Journeys
                    if actions['new_journeys']:
                        logic = """
                            UNWIND $batch as row
                            CREATE (j:Journey {id: row.id, created_at: row.created_at})
                        """
                        self._exec_parameterized_cypher(cursor, logic, actions['new_journeys'])

                    # B. Link CIDs to Journeys
                    if actions['cid_links']:
                        logic = """
                            UNWIND $batch as row
                            MATCH (j:Journey {id: row.jid})
                            MERGE (c:Correlation {id: row.cid})
                            MERGE (c)-[:PART_OF]->(j)
                        """
                        self._exec_parameterized_cypher(cursor, logic, actions['cid_links'])

                    # C. Merges
                    if actions['merges']:
                        # Phase 1: Rewire
                        logic_rewire = """
                            UNWIND $batch as row
                            MATCH (loser:Journey {id: row.loser})
                            MATCH (winner:Journey {id: row.winner})
                            MATCH (c:Correlation)-[r:PART_OF]->(loser)
                            DELETE r
                            MERGE (c)-[:PART_OF]->(winner)
                        """
                        self._exec_parameterized_cypher(cursor, logic_rewire, actions['merges'])
                        
                        # Phase 2: Delete
                        logic_del = """
                            UNWIND $batch as row
                            MATCH (loser:Journey {id: row.loser})
                            DETACH DELETE loser
                        """
                        self._exec_parameterized_cypher(cursor, logic_del, actions['merges'])

                    # D. Update Status
                    if actions['event_ids']:
                        logic_update = """
                            UNWIND $batch as eid
                            MATCH (e:Event {id: eid})
                            SET e.status = 'PROCESSED'
                        """
                        self._exec_parameterized_cypher(cursor, logic_update, actions['event_ids'])

                elapsed = time.time() - start_time
                logger.info(f"Batch completed in {elapsed:.2f}s")

            finally:
                self._return_connection(conn)

    def _compute_graph_actions(self, events, all_cids, cid_to_journeys, existing_journeys_map):
        parent = {}
        def find(i):
            if i not in parent: parent[i] = i
            if parent[i] != i: parent[i] = find(parent[i])
            return parent[i]
        def union(i, j):
            root_i, root_j = find(i), find(j)
            if root_i != root_j: parent[root_i] = root_j

        for ev in events:
            cids = ev['cids']
            if not cids: continue
            first = cids[0]
            for other in cids[1:]: union(first, other)
            for cid in cids:
                if cid in cid_to_journeys:
                    for jid in cid_to_journeys[cid]: union(cid, jid)

        groups = defaultdict(lambda: {'cids': set(), 'jids': set()})
        for cid in all_cids:
            groups[find(cid)]['cids'].add(cid)
        for jid in existing_journeys_map:
            if jid in parent:
                groups[find(jid)]['jids'].add(jid)

        res = {'new_journeys': [], 'merges': [], 'cid_links': [], 'event_ids': [e['id'] for e in events]}
        
        for group in groups.values():
            jids = list(group['jids'])
            target_jid = None
            
            if not jids:
                target_jid = f"journey_{uuid.uuid4()}"
                res['new_journeys'].append({'id': target_jid, 'created_at': datetime.now().isoformat()})
            else:
                jids.sort(key=lambda x: existing_journeys_map.get(x, ""))
                target_jid = jids[0]
                for loser in jids[1:]:
                    res['merges'].append({'winner': target_jid, 'loser': loser})
            
            for cid in group['cids']:
                res['cid_links'].append({'jid': target_jid, 'cid': cid})
                
        return res

    def get_journey(self, event_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            # Wrap for named parameter
            wrapper = {"batch": event_id}
            wrapper_json = json.dumps(wrapper)

            logic = """
                MATCH (start_e:Event {id: $batch})-[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j:Journey)
                MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(all_e:Event)
                RETURN j.id, collect(DISTINCT all_e.id)
            """
            
            stmt_name = f"stmt_get_{uuid.uuid4().hex}"
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    PREPARE {stmt_name} (agtype) AS 
                    SELECT * FROM cypher('{self.graph_name}', $${logic}$$, $1) 
                    as (jid agtype, events agtype);
                """)
                cursor.execute(f"EXECUTE {stmt_name} (%s::agtype)", (wrapper_json,))
                row = cursor.fetchone()
                cursor.execute(f"DEALLOCATE {stmt_name}")
                
                if row:
                    jid = row[0]
                    events = row[1]
                    if isinstance(jid, str): jid = json.loads(jid)
                    if isinstance(events, str): events = json.loads(events)
                    return {"journey_id": jid, "events": events}
                return None
        except Exception as e:
            logger.error(f"get_journey failed: {e}")
            raise
        finally:
            self._return_connection(conn)

    def close(self):
        if self.connection_pool:
            self.connection_pool.closeall()

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

    jm = ApacheAgeJourneyManager(DB_CONFIG)
    jm.clean()
    
    logger.info("Starting High-Load Benchmark")
    generated_data, ingest_time = generate_traffic(jm, 1000, 1, 5, 2000) 
    
    t0 = time.time()
    jm.process_events()
    process_time = time.time() - t0
    
    logger.info(f"Ingest: {ingest_time:.2f}s, Process: {process_time:.2f}s")
    
    validate_stitching(jm, generated_data)
    
    jm.close()