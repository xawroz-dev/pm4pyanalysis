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
            session.run("CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE")
            session.run("CREATE CONSTRAINT journey_id IF NOT EXISTS FOR (j:Journey) REQUIRE j.id IS UNIQUE")
            session.run("CREATE INDEX event_status IF NOT EXISTS FOR (e:Event) ON (e.status)")
    
    def clean(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        self.setup()
    
    def ingest_batch(self, events_batch):
        if not events_batch:
            return
            
        with self.driver.session() as session:
            session.run("""
                UNWIND $events AS event
                CREATE (e:Event {
                    id: event.id,
                    correlation_ids: event.correlation_ids,
                    status: 'NEW',
                    created_at: timestamp()
                })
            """, events=events_batch)
    
    def process_events(self):
        with self.driver.session() as session:
            # Get all NEW events
            result = session.run("""
                MATCH (e:Event {status: 'NEW'})
                RETURN e.id AS id, e.correlation_ids AS c_ids
            """)
            
            new_events = [{"id": record["id"], "c_ids": set(record["c_ids"])} for record in result]
            
            if not new_events:
                return
            
            # In-memory clustering
            from collections import defaultdict
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
            
            # Process each cluster
            for cluster_indices in clusters:
                cluster_events = [new_events[i] for i in cluster_indices]
                all_cids = set()
                for ev in cluster_events:
                    all_cids.update(ev['c_ids'])
                
                event_ids = [ev['id'] for ev in cluster_events]
                
                # Find existing journeys
                result = session.run("""
                    MATCH (e:Event)-[:BELONGS_TO]->(j:Journey)
                    WHERE e.status = 'PROCESSED'
                    AND any(cid IN e.correlation_ids WHERE cid IN $all_cids)
                    RETURN DISTINCT j.id AS jid, j.created_at AS jcreated
                    ORDER BY j.created_at
                """, all_cids=list(all_cids))
                
                existing_journeys = [{"id": record["jid"], "created_at": record["jcreated"]} for record in result]
                
                if not existing_journeys:
                    # Create new journey
                    journey_id = f"journey_{uuid.uuid4()}"
                    session.run("""
                        CREATE (j:Journey {id: $jid, created_at: timestamp()})
                    """, jid=journey_id)
                    
                elif len(existing_journeys) == 1:
                    journey_id = existing_journeys[0]['id']
                    
                else:
                    # Merge journeys
                    journey_id = existing_journeys[0]['id']
                    loser_ids = [j['id'] for j in existing_journeys[1:]]
                    
                    session.run("""
                        MATCH (e:Event)-[r:BELONGS_TO]->(old_j:Journey)
                        WHERE old_j.id IN $loser_ids
                        MATCH (new_j:Journey {id: $winner_id})
                        DELETE r
                        CREATE (e)-[:BELONGS_TO]->(new_j)
                    """, loser_ids=loser_ids, winner_id=journey_id)
                    
                    session.run("""
                        MATCH (j:Journey)
                        WHERE j.id IN $loser_ids
                        DELETE j
                    """, loser_ids=loser_ids)
                
                # Link events to journey
                session.run("""
                    MATCH (j:Journey {id: $jid})
                    MATCH (e:Event)
                    WHERE e.id IN $event_ids
                    CREATE (e)-[:BELONGS_TO]->(j)
                    SET e.status = 'PROCESSED'
                """, jid=journey_id, event_ids=event_ids)
    
    def get_journey(self, event_id):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (e:Event {id: $eid})-[:BELONGS_TO]->(j:Journey)
                MATCH (all_e:Event)-[:BELONGS_TO]->(j)
                RETURN j.id AS jid, collect(all_e.id) AS event_ids
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
    EVENTS_PER_JOURNEY = 5
    
    print("Starting Benchmark: Neo4j")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_JOURNEY)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)
    jm.close()
