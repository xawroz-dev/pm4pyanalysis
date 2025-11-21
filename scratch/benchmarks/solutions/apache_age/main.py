import psycopg2
import json
import time
import re
import uuid
from datetime import datetime
from collections import defaultdict
import sys
import os

# Add common directory to path to import interface
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

class ApacheAgeJourneyManager(JourneyManager):
    def __init__(self, db_config):
        self.db_config = db_config
        self.graph_name = "benchmark_graph"
        self.conn = None

    def connect(self):
        if not self.conn or self.conn.closed:
            try:
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = True
            except Exception as e:
                print(f"Error connecting to database: {e}")
                raise

    def _execute_cypher(self, query, params=None):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            
            full_query = f"SELECT * FROM cypher('{self.graph_name}', $$ {query} $$) as (v agtype);"
            try:
                cursor.execute(full_query)
                return cursor.fetchall()
            except Exception as e:
                # print(f"Cypher Error: {e}")
                raise e

    def setup(self):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            
            cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"SELECT create_graph('{self.graph_name}');")
            
            self._execute_cypher("CREATE (:Event), (:Journey)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_event_id ON {self.graph_name}.\"Event\" USING GIN (properties);")

    def clean(self):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            cursor.execute(f"SELECT drop_graph('{self.graph_name}', true);")
        self.setup()

    def ingest_batch(self, events_batch):
        if not events_batch:
            return

        self.connect()
        creates = []
        for ev in events_batch:
            props = {
                "id": ev['id'],
                "correlation_ids": ev['correlation_ids'],
                "status": "NEW",
                "created_at": datetime.now().isoformat(),
                "payload": ev.get('payload', {})
            }
            props_json = json.dumps(props)
            safe_props_json = re.sub(r'"([a-zA-Z0-9_]+)":', r'\1:', props_json)
            creates.append(f"(:Event {safe_props_json})")
        
        # Chunking to avoid huge queries
        chunk_size = 500
        for i in range(0, len(creates), chunk_size):
            chunk = creates[i:i+chunk_size]
            query = "CREATE " + ", ".join(chunk)
            self._execute_cypher(query)

    def process_events(self):
        self.connect()
        
        # 1. Fetch all NEW events
        fetch_query = "MATCH (e:Event) WHERE e.status = 'NEW' RETURN e.id, e.correlation_ids"
        
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            cursor.execute(f"SELECT * FROM cypher('{self.graph_name}', $$ {fetch_query} $$) as (id agtype, c_ids agtype);")
            rows = cursor.fetchall()
            
        if not rows:
            return

        new_events = []
        for r in rows:
            eid = json.loads(r[0])
            cids = set(json.loads(r[1]))
            new_events.append({'id': eid, 'c_ids': cids})

        # 2. In-Memory Clustering
        cid_to_events = defaultdict(list)
        for idx, ev in enumerate(new_events):
            for cid in ev['c_ids']:
                cid_to_events[cid].append(idx)
        
        visited = [False] * len(new_events)
        clusters = []
        
        for i in range(len(new_events)):
            if visited[i]:
                continue
            
            component_indices = []
            stack = [i]
            visited[i] = True
            
            while stack:
                curr_idx = stack.pop()
                component_indices.append(curr_idx)
                
                for cid in new_events[curr_idx]['c_ids']:
                    for neighbor_idx in cid_to_events[cid]:
                        if not visited[neighbor_idx]:
                            visited[neighbor_idx] = True
                            stack.append(neighbor_idx)
            
            clusters.append(component_indices)
            
        # 3. Process Clusters
        for cluster_indices in clusters:
            cluster_events = [new_events[i] for i in cluster_indices]
            all_cids = set()
            for ev in cluster_events:
                all_cids.update(ev['c_ids'])
            
            cids_json = json.dumps(list(all_cids)).replace("'", "''")
            
            find_j_query = f"""
                MATCH (e:Event)-[:BELONGS_TO]->(j:Journey)
                WHERE e.status = 'PROCESSED'
                AND size([x IN e.correlation_ids WHERE x IN {cids_json}]) > 0
                RETURN DISTINCT j.id, j.created_at
            """
            
            with self.conn.cursor() as cursor:
                cursor.execute("LOAD 'age';")
                cursor.execute("SET search_path = ag_catalog, '$user', public;")
                cursor.execute(f"SELECT * FROM cypher('{self.graph_name}', $$ {find_j_query} $$) as (jid agtype, jcreated agtype);")
                existing_journeys_rows = cursor.fetchall()
            
            target_journey_id = None
            
            if not existing_journeys_rows:
                target_journey_id = f"journey_{uuid.uuid4()}"
                created_at = datetime.now().isoformat()
                self._execute_cypher(f"CREATE (j:Journey {{id: '{target_journey_id}', created_at: '{created_at}'}})")
            
            elif len(existing_journeys_rows) == 1:
                target_journey_id = json.loads(existing_journeys_rows[0][0])
                
            else:
                parsed_journeys = []
                for row in existing_journeys_rows:
                    parsed_journeys.append({'id': json.loads(row[0]), 'created_at': json.loads(row[1])})
                parsed_journeys.sort(key=lambda x: x['created_at'])
                
                winner = parsed_journeys[0]
                target_journey_id = winner['id']
                losers = parsed_journeys[1:]
                
                for loser in losers:
                    loser_id = loser['id']
                    move_query = f"""
                        MATCH (e:Event)-[r:BELONGS_TO]->(old_j:Journey {{id: '{loser_id}'}})
                        MATCH (new_j:Journey {{id: '{target_journey_id}'}})
                        DELETE r
                        CREATE (e)-[:BELONGS_TO]->(new_j)
                    """
                    self._execute_cypher(move_query)
                    self._execute_cypher(f"MATCH (j:Journey {{id: '{loser_id}'}}) DELETE j")

            event_ids = [ev['id'] for ev in cluster_events]
            # Batch link
            # Chunking for safety
            chunk_size = 100
            for i in range(0, len(event_ids), chunk_size):
                chunk = event_ids[i:i+chunk_size]
                eids_json = json.dumps(chunk).replace("'", "''")
                link_query = f"""
                    MATCH (j:Journey {{id: '{target_journey_id}'}})
                    MATCH (e:Event)
                    WHERE size([x IN [e.id] WHERE x IN {eids_json}]) > 0
                    CREATE (e)-[:BELONGS_TO]->(j)
                    SET e.status = 'PROCESSED'
                """
                self._execute_cypher(link_query)

    def get_journey(self, event_id):
        query = f"""
            MATCH (e:Event {{id: '{event_id}'}})-[:BELONGS_TO]->(j:Journey)
            MATCH (all_e:Event)-[:BELONGS_TO]->(j)
            RETURN j.id, collect(all_e.id)
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            cursor.execute(f"SELECT * FROM cypher('{self.graph_name}', $$ {query} $$) as (jid agtype, event_ids agtype);")
            result = cursor.fetchone()
            
        if result:
            return {
                "journey_id": json.loads(result[0]),
                "events": json.loads(result[1])
            }
        return None

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
    
    jm = ApacheAgeJourneyManager(DB_CONFIG)
    
    # Wait for DB
    for i in range(10):
        try:
            jm.connect()
            break
        except:
            time.sleep(2)
            
    jm.setup()
    jm.clean()
    
    # Benchmark
    NUM_JOURNEYS = 1000
    EVENTS_PER_JOURNEY = 5
    
    print("Starting Benchmark: Apache AGE")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_JOURNEY)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)
