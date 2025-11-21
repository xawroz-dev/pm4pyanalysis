import time
import uuid
from datetime import datetime
from arango import ArangoClient
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

class ArangoDBJourneyManager(JourneyManager):
    def __init__(self, host, username, password):
        self.client = ArangoClient(hosts=host)
        self.sys_db = self.client.db('_system', username=username, password=password)
        self.db_name = 'benchmark_db'
        self.graph_name = 'journey_graph'
        
    def setup(self):
        # Create database
        if not self.sys_db.has_database(self.db_name):
            self.sys_db.create_database(self.db_name)
        
        self.db = self.client.db(self.db_name, username='root', password='password')
        
        # Create collections
        if not self.db.has_collection('events'):
            self.db.create_collection('events')
        if not self.db.has_collection('journeys'):
            self.db.create_collection('journeys')
        if not self.db.has_collection('belongs_to'):
            self.db.create_collection('belongs_to', edge=True)
        
        # Create graph
        if not self.db.has_graph(self.graph_name):
            self.db.create_graph(
                self.graph_name,
                edge_definitions=[{
                    'edge_collection': 'belongs_to',
                    'from_vertex_collections': ['events'],
                    'to_vertex_collections': ['journeys']
                }]
            )
    
    def clean(self):
        if self.sys_db.has_database(self.db_name):
            self.sys_db.delete_database(self.db_name)
        self.setup()
    
    def ingest_batch(self, events_batch):
        if not events_batch:
            return
        
        events_col = self.db.collection('events')
        docs = []
        for ev in events_batch:
            docs.append({
                '_key': ev['id'].replace('_', '-'),
                'id': ev['id'],
                'correlation_ids': ev['correlation_ids'],
                'status': 'NEW',
                'created_at': datetime.now().isoformat(),
                'payload': ev.get('payload', {})
            })
        
        # Batch insert
        events_col.insert_many(docs)
    
    def process_events(self):
        events_col = self.db.collection('events')
        
        # Get all NEW events
        cursor = self.db.aql.execute("""
            FOR e IN events
            FILTER e.status == 'NEW'
            RETURN {id: e.id, c_ids: e.correlation_ids}
        """)
        
        new_events = [{"id": doc["id"], "c_ids": set(doc["c_ids"])} for doc in cursor]
        
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
        journeys_col = self.db.collection('journeys')
        belongs_to_col = self.db.collection('belongs_to')
        
        for cluster_indices in clusters:
            cluster_events = [new_events[i] for i in cluster_indices]
            all_cids = set()
            for ev in cluster_events:
                all_cids.update(ev['c_ids'])
            
            event_ids = [ev['id'] for ev in cluster_events]
            
            # Find existing journeys
            cursor = self.db.aql.execute("""
                FOR e IN events
                FILTER e.status == 'PROCESSED'
                FILTER LENGTH(INTERSECTION(e.correlation_ids, @all_cids)) > 0
                FOR v, e_edge IN 1..1 OUTBOUND e belongs_to
                RETURN DISTINCT {id: v.id, created_at: v.created_at}
            """, bind_vars={'all_cids': list(all_cids)})
            
            existing_journeys = sorted([doc for doc in cursor], key=lambda x: x['created_at'])
            
            if not existing_journeys:
                # Create new journey
                journey_id = f"journey_{uuid.uuid4()}"
                journeys_col.insert({
                    '_key': journey_id.replace('_', '-'),
                    'id': journey_id,
                    'created_at': datetime.now().isoformat()
                })
                
            elif len(existing_journeys) == 1:
                journey_id = existing_journeys[0]['id']
                
            else:
                # Merge journeys
                journey_id = existing_journeys[0]['id']
                loser_ids = [j['id'] for j in existing_journeys[1:]]
                
                # Move edges
                for loser_id in loser_ids:
                    self.db.aql.execute("""
                        FOR e IN events
                        FOR v, e_edge IN 1..1 OUTBOUND e belongs_to
                        FILTER v.id == @loser_id
                        REMOVE e_edge IN belongs_to
                        INSERT {_from: e._id, _to: CONCAT('journeys/', @winner_key)} INTO belongs_to
                    """, bind_vars={'loser_id': loser_id, 'winner_key': journey_id.replace('_', '-')})
                    
                    # Delete old journey
                    journeys_col.delete(loser_id.replace('_', '-'))
            
            # Link events to journey
            journey_key = journey_id.replace('_', '-')
            for eid in event_ids:
                event_key = eid.replace('_', '-')
                belongs_to_col.insert({
                    '_from': f'events/{event_key}',
                    '_to': f'journeys/{journey_key}'
                })
                events_col.update({'_key': event_key}, {'status': 'PROCESSED'})
    
    def get_journey(self, event_id):
        event_key = event_id.replace('_', '-')
        cursor = self.db.aql.execute("""
            FOR v, e IN 1..1 OUTBOUND @event_id belongs_to
            LET all_events = (
                FOR ev, edge IN 1..1 INBOUND v belongs_to
                RETURN ev.id
            )
            RETURN {journey_id: v.id, events: all_events}
        """, bind_vars={'event_id': f'events/{event_key}'})
        
        result = cursor.next() if cursor.count() > 0 else None
        return result

if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching
    
    jm = ArangoDBJourneyManager('http://localhost:8529', 'root', 'password')
    
    # Wait for DB
    for i in range(30):
        try:
            jm.setup()
            break
        except:
            time.sleep(2)
    
    jm.clean()
    
    NUM_JOURNEYS = 100
    EVENTS_PER_APP = 5
    NUM_APPS = 4
    
    print("Starting Benchmark: ArangoDB")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)

