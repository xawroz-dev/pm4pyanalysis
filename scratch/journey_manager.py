import psycopg2
import json
import time
from datetime import datetime
import re
from collections import defaultdict
import uuid

class JourneyManager:
    def __init__(self, db_config):
        self.db_config = db_config
        self.graph_name = "event_journey_graph"
        self.conn = None

    def connect(self):
        if not self.conn or self.conn.closed:
            try:
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = True
            except Exception as e:
                print(f"Error connecting to database: {e}")
                raise

    def setup_graph(self):
        """Initializes the AGE extension and creates the graph if not exists."""
        self.connect()
        with self.conn.cursor() as cursor:
            # Load AGE extension
            cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            
            # Create graph if not exists
            # Check if graph exists first to avoid error
            cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"SELECT create_graph('{self.graph_name}');")
                print(f"Graph '{self.graph_name}' created.")
            else:
                print(f"Graph '{self.graph_name}' already exists.")

            # Create indexes (using standard Postgres indexes on the vertex tables)
            # Note: In AGE, labels are tables in the graph's schema.
            # We need to ensure the labels exist first by creating a dummy node or just creating the label.
            # Let's create labels first via cypher
            self._execute_cypher("CREATE (:Event), (:Journey)")
            
            # Now create GIN indices for faster JSONB searching on properties
            # We assume the schema name is the graph name
            # Index for Event id
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_event_id ON {self.graph_name}.\"Event\" USING GIN (properties);")
            
    def clean_data(self):
        """Drops and recreates the graph for a clean state."""
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            cursor.execute(f"SELECT drop_graph('{self.graph_name}', true);")
            print("Graph dropped.")
        self.setup_graph()

    def _execute_cypher(self, query, params=None):
        """Helper to execute Cypher queries."""
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            
            # Format params for Cypher if needed, but AGE usually takes literals or we pass them carefully.
            # For safety with psycopg2, we wrap the cypher call.
            # query should be the cypher string inside $$ $$
            
            full_query = f"SELECT * FROM cypher('{self.graph_name}', $$ {query} $$) as (v agtype);"
            if params:
                # If we need to pass parameters, AGE supports passing a parameter map as the second arg to cypher()
                # cypher('graph_name', $$ ... $$, ? )
                # But the python driver support for that can be tricky with raw psycopg2. 
                # We will format the string carefully for this demo or use simple interpolation for literals.
                pass
            
            try:
                cursor.execute(full_query)
                return cursor.fetchall()
            except Exception as e:
                print(f"Cypher Error: {e}")
                raise e

    def ingest_events_batch(self, events_batch):
        """
        Ingests a batch of events.
        events_batch: list of dicts {'id': str, 'correlation_ids': list, 'payload': dict}
        """
        if not events_batch:
            return

        self.connect()
        
        # We can construct a large CREATE statement or use UNWIND if we could pass parameters easily.
        # For raw psycopg2/AGE string construction, we'll build a multi-CREATE query.
        # To avoid query size limits, we can chunk it, but let's assume reasonable batch sizes (e.g. 1000).
        
        # Construct query: CREATE (:Event {...}), (:Event {...}), ...
        
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
            # Regex to unquote keys for Cypher map compatibility
            safe_props_json = re.sub(r'"([a-zA-Z0-9_]+)":', r'\1:', props_json)
            creates.append(f"(:Event {safe_props_json})")
        
        query = "CREATE " + ", ".join(creates)
        self._execute_cypher(query)
        # print(f"Ingested batch of {len(events_batch)} events.")

    def process_new_events(self):
        """
        Optimized processing:
        1. Fetch ALL NEW events (id, correlation_ids).
        2. Cluster them in-memory (Union-Find).
        3. For each cluster, find related existing Journeys (Batch Query).
        4. Resolve (Create/Link/Merge) in batches.
        """
        self.connect()
        
        # 1. Fetch all NEW events
        fetch_query = "MATCH (e:Event) WHERE e.status = 'NEW' RETURN e.id, e.correlation_ids"
        
        with self.conn.cursor() as cursor:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            cursor.execute(f"SELECT * FROM cypher('{self.graph_name}', $$ {fetch_query} $$) as (id agtype, c_ids agtype);")
            rows = cursor.fetchall()
            
        if not rows:
            print("No new events to process.")
            return

        print(f"Processing {len(rows)} new events...")
        
        # Parse rows
        new_events = [] # list of {'id': str, 'c_ids': set}
        for r in rows:
            eid = json.loads(r[0])
            cids = set(json.loads(r[1]))
            new_events.append({'id': eid, 'c_ids': cids})

        # 2. In-Memory Clustering (Union-Find)
        # We want to group events that share ANY correlation ID.
        # Map: correlation_id -> list of event_indices
        cid_to_events = defaultdict(list)
        for idx, ev in enumerate(new_events):
            for cid in ev['c_ids']:
                cid_to_events[cid].append(idx)
        
        # BFS/DFS to find components
        visited = [False] * len(new_events)
        clusters = []
        
        for i in range(len(new_events)):
            if visited[i]:
                continue
            
            # Start a new cluster
            component_indices = []
            stack = [i]
            visited[i] = True
            
            while stack:
                curr_idx = stack.pop()
                component_indices.append(curr_idx)
                
                # Find neighbors via correlation IDs
                for cid in new_events[curr_idx]['c_ids']:
                    for neighbor_idx in cid_to_events[cid]:
                        if not visited[neighbor_idx]:
                            visited[neighbor_idx] = True
                            stack.append(neighbor_idx)
            
            clusters.append(component_indices)
            
        print(f"Found {len(clusters)} clusters of new events.")
        
        # 3. Process Clusters
        # For each cluster, we need to check if it connects to any EXISTING journeys.
        # We can do this by collecting all correlation IDs in the cluster and querying the DB.
        
        for cluster_indices in clusters:
            cluster_events = [new_events[i] for i in cluster_indices]
            all_cids = set()
            for ev in cluster_events:
                all_cids.update(ev['c_ids'])
            
            # Query DB for existing journeys matching these CIDs
            # MATCH (e:Event)-[:BELONGS_TO]->(j:Journey)
            # WHERE e.status = 'PROCESSED' AND size([x IN e.correlation_ids WHERE x IN $all_cids]) > 0
            # RETURN DISTINCT j.id, j.created_at
            
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
                # Create new journey
                target_journey_id = f"journey_{uuid.uuid4()}"
                created_at = datetime.now().isoformat()
                self._execute_cypher(f"CREATE (j:Journey {{id: '{target_journey_id}', created_at: '{created_at}'}})")
                # print(f"  Created {target_journey_id}")
            
            elif len(existing_journeys_rows) == 1:
                # Link to existing
                target_journey_id = json.loads(existing_journeys_rows[0][0])
                # print(f"  Linking to {target_journey_id}")
                
            else:
                # Merge
                parsed_journeys = []
                for row in existing_journeys_rows:
                    parsed_journeys.append({'id': json.loads(row[0]), 'created_at': json.loads(row[1])})
                parsed_journeys.sort(key=lambda x: x['created_at'])
                
                winner = parsed_journeys[0]
                target_journey_id = winner['id']
                losers = parsed_journeys[1:]
                
                # print(f"  Merging {len(losers)} into {target_journey_id}")
                
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

            # Link all new events in this cluster to target_journey_id
            # We can batch this too
            # MATCH (j:Journey {id: ...})
            # MATCH (e:Event) WHERE e.id IN [...]
            # CREATE (e)-[:BELONGS_TO]->(j)
            # SET e.status = 'PROCESSED'
            
            event_ids = [ev['id'] for ev in cluster_events]
            eids_json = json.dumps(event_ids).replace("'", "''")
            
            link_query = f"""
                MATCH (j:Journey {{id: '{target_journey_id}'}})
                MATCH (e:Event)
                WHERE size([x IN [e.id] WHERE x IN {eids_json}]) > 0
                CREATE (e)-[:BELONGS_TO]->(j)
                SET e.status = 'PROCESSED'
            """
            self._execute_cypher(link_query)

    def get_journey(self, event_id):
        """
        Given an event_id, return the Journey ID and all connected events.
        """
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
