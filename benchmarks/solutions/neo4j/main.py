import time
import uuid
from datetime import datetime
from neo4j import GraphDatabase
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

class Neo4jJourneyManager(JourneyManager):
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def setup(self):
        with self.driver.session() as session:
            # Constraints
            session.run("CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE")
            session.run("CREATE CONSTRAINT journey_id IF NOT EXISTS FOR (j:Journey) REQUIRE j.id IS UNIQUE")
            session.run("CREATE CONSTRAINT correlation_id IF NOT EXISTS FOR (c:Correlation) REQUIRE c.id IS UNIQUE")
            
            # Indexes
            session.run("CREATE INDEX event_status IF NOT EXISTS FOR (e:Event) ON (e.status)")
            session.run("CREATE INDEX correlation_component IF NOT EXISTS FOR (c:Correlation) ON (c.componentId)")
            session.run("CREATE INDEX event_component IF NOT EXISTS FOR (e:Event) ON (e.componentId)")
    
    def clean(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            # Drop GDS graph if exists
            try:
                session.run("CALL gds.graph.drop('stitching', false) YIELD graphName")
            except:
                pass
        self.setup()
    
    def ingest_batch(self, events_batch):
        if not events_batch:
            return
            
        # Pre-process payload to string
        processed_batch = []
        for ev in events_batch:
            ev_copy = ev.copy()
            ev_copy['payload'] = str(ev.get('payload', {}))
            processed_batch.append(ev_copy)
            
        with self.driver.session() as session:
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
        with self.driver.session() as session:
            # 1. Project Graph for WCC
            # We project Events and Correlations and their connections
            # We only need to consider NEW events and their connections, plus existing structure
            # For simplicity in this benchmark, we project the whole relevant subgraph
            
            # Check if graph exists and drop it
            session.run("CALL gds.graph.drop('stitching', false) YIELD graphName")
            
            # Project graph
            # We project Event and Correlation nodes, and HAS_KEY relationships (undirected for WCC)
            session.run("""
                CALL gds.graph.project(
                    'stitching',
                    ['Event', 'Correlation'],
                    {
                        HAS_KEY: {orientation: 'UNDIRECTED'}
                    }
                )
            """)
            
            # 2. Run WCC and write componentId back to nodes
            session.run("""
                CALL gds.wcc.write(
                    'stitching',
                    { writeProperty: 'componentId' }
                )
            """)
            
            # 3. Process Components with NEW events
            # Find components that contain at least one NEW event
            # For each such component, resolve the journey
            
            # We can do this in a batched Cypher query
            
            # Get distinct componentIds that have NEW events
            result = session.run("""
                MATCH (e:Event {status: 'NEW'})
                RETURN DISTINCT e.componentId as cid
            """)
            
            component_ids = [record['cid'] for record in result]
            
            if not component_ids:
                return

            # Process in batches of components to avoid huge transactions
            BATCH_SIZE = 100
            for i in range(0, len(component_ids), BATCH_SIZE):
                batch_cids = component_ids[i:i+BATCH_SIZE]
            
                
                session.run("""
                    UNWIND $cids as target_cid
                    
                    MATCH (c:Correlation {componentId: target_cid})
                    WITH target_cid, collect(c) as correlations
                    
                    // Find existing journeys
                    OPTIONAL MATCH (c_existing:Correlation {componentId: target_cid})-[:PART_OF]->(j:Journey)
                    WITH target_cid, correlations, collect(DISTINCT j) as existing_journeys
                    
                    // Select Winner (Oldest)
                    // We need to sort existing_journeys. Since we can't sort in list comprehension easily,
                    // we'll use UNWIND if there are journeys, or null if empty.
                    
                    CALL apoc.do.case(
                        [
                            size(existing_journeys) > 0,
                            'UNWIND existing_journeys as j WITH j ORDER BY j.created_at ASC LIMIT 1 RETURN j as winner',
                            
                            size(existing_journeys) = 0,
                            'RETURN null as winner'
                        ],
                        'RETURN null as winner',
                        {existing_journeys: existing_journeys}
                    ) YIELD value
                    WITH target_cid, correlations, existing_journeys, value.winner as found_winner
                    
                    // Create new if needed
                    CALL apoc.do.when(
                        found_winner IS NULL,
                        'CREATE (j:Journey {id: "journey_" + randomUUID(), created_at: timestamp()}) RETURN j',
                        'RETURN found_winner as j',
                        {found_winner: found_winner}
                    ) YIELD value
                    WITH target_cid, correlations, existing_journeys, value.j as winner_journey
                    
                    // Link Correlations
                    FOREACH (c IN correlations | MERGE (c)-[:PART_OF]->(winner_journey))
                    
                    // Delete Losers
                    FOREACH (loser IN [j IN existing_journeys WHERE j <> winner_journey] | DETACH DELETE loser)
                """, cids=batch_cids)
                
                # Mark events processed for this batch
                session.run("""
                    UNWIND $cids as target_cid
                    MATCH (e:Event {componentId: target_cid, status: 'NEW'})
                    SET e.status = 'PROCESSED'
                """, cids=batch_cids)
            
            # Cleanup
            session.run("CALL gds.graph.drop('stitching', false) YIELD graphName")

    def get_journey(self, event_id):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (e:Event {id: $eid})-[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j:Journey)
                MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(all_e:Event)
                RETURN j.id AS jid, collect(DISTINCT all_e.id) AS event_ids
            """, eid=event_id)
            
            record = result.single()
            if record:
                return {
                    "journey_id": record["jid"],
                    "events": record["event_ids"]
                }
            return None

if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching
    
    jm = Neo4jJourneyManager("bolt://localhost:7687", "neo4j", "password")
    
    # Wait for DB
    for i in range(30):
        try:
            jm.setup()
            break
        except:
            time.sleep(2)
    
    jm.clean()
    
    NUM_JOURNEYS = 1000
    EVENTS_PER_APP = 50
    NUM_APPS = 5
    
    print("Starting Benchmark: Neo4j (WCC Optimized)")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)
    jm.close()
