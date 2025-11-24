import time
import uuid
import json
from datetime import datetime
from falkordb import FalkorDB
import sys
import os

# Add common directory to path to import interface
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

class FalkorDBJourneyManager(JourneyManager):
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.db = None
        self.graph_name = 'benchmark_graph'
        
    def connect(self):
        if not self.db:
            try:
                self.client = FalkorDB(host=self.host, port=self.port)
                self.db = self.client.select_graph(self.graph_name)
            except Exception as e:
                print(f"Error connecting to FalkorDB: {e}")
                raise

    def setup(self):
        self.connect()
        # Standard Indices
        try:
            self.db.query("CREATE INDEX FOR (e:Event) ON (e.id)")
        except Exception: pass
        
        try:
            self.db.query("CREATE INDEX FOR (c:Correlation) ON (c.id)")
        except Exception: pass

        # NEW: Index for the WCC component ID to speed up the final aggregation
        try:
            self.db.query("CREATE INDEX FOR (n:Event) ON (n.wcc_id)")
        except Exception: pass
        
        try:
            self.db.query("CREATE INDEX FOR (c:Correlation) ON (c.wcc_id)")
        except Exception: pass

    def clean(self):
        self.connect()
        try:
            self.db.delete()
        except:
            pass
        self.setup()

    def ingest_batch(self, events_batch):
        """
        Optimized Ingest:
        Blind writes only. We do NOT look up journeys here. 
        We simply link Events to Correlation keys.
        """
        if not events_batch:
            return
        
        self.connect()
        
        for ev in events_batch:
            event_id = ev['id']
            correlation_ids = ev['correlation_ids']
            payload = json.dumps(ev.get('payload', {}))
            created_at = datetime.now().isoformat()
            
            query = """
                CREATE (e:Event {
                    id: $id, 
                    created_at: $created_at,
                    payload: $payload
                })
                WITH e
                UNWIND $cids as cid
                MERGE (c:Correlation {id: cid})
                MERGE (e)-[:HAS_KEY]->(c)
            """
            
            params = {
                'id': event_id,
                'created_at': created_at,
                'payload': payload,
                'cids': correlation_ids
            }
            
            self.db.query(query, params)

    def process_events(self):
        """
        Optimized Process:
        1. Uses FalkorDB Algo WCC to identify connected components (clusters).
        2. Writes a 'wcc_id' property to all nodes in the graph.
        3. Aggregates nodes by 'wcc_id' and creates/merges the Journey node in one go.
        """
        self.connect()
        
        # 1. Run Weakly Connected Components (WCC)
        # This mathematically finds all events that share keys (directly or indirectly)
        # and assigns them a common integer ID (wcc_id).
        print("Running WCC Algorithm...")
        try:
            # Note: algo.wcc treats the graph as undirected by default for connectivity checks
            self.db.query("CALL algo.wcc({write: true, writeProperty: 'wcc_id'})")
        except Exception as e:
            print(f"WCC Failed. Ensure FalkorDB module is loaded. Error: {e}")
            return

        # 2. Materialize Journeys based on WCC IDs
        # We group by the calculated wcc_id, create a Journey, and link everything.
        print("Structuring Journeys from WCC components...")
        structure_query = """
            MATCH (e:Event)
            WHERE e.wcc_id IS NOT NULL
            WITH e.wcc_id as cid, collect(e) as events, min(e.created_at) as start_time
            
            MERGE (j:Journey {id: 'journey_' + toString(cid)})
            ON CREATE SET j.created_at = start_time
            
            WITH j, events
            UNWIND events as event
            MERGE (event)-[:PART_OF]->(j)
            
            WITH event
            MATCH (event)-[:HAS_KEY]->(c:Correlation)
            MERGE (c)-[:PART_OF]->(j)
        """
        self.db.query(structure_query)

    def get_journey(self, event_id):
        # Logic remains mostly same, but traverse PART_OF to find siblings
        query = """
            MATCH (e:Event {id: $eid})-[:PART_OF]->(j:Journey)
            MATCH (all_e:Event)-[:PART_OF]->(j)
            RETURN j.id, collect(DISTINCT all_e.id)
        """
        
        result = self.db.query(query, {'eid': event_id})
        if result.result_set:
            row = result.result_set[0]
            return {
                "journey_id": row[0],
                "events": row[1]
            }
        return None

if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching
    
    jm = FalkorDBJourneyManager()
    
    # Wait for DB
    for i in range(30):
        try:
            jm.connect()
            break
        except:
            time.sleep(2)
            
    jm.setup()
    jm.clean()
    
    NUM_JOURNEYS = 1000
    EVENTS_PER_APP = 5
    NUM_APPS = 4
    
    print("Starting Benchmark: FalkorDB (WCC Optimized)")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)