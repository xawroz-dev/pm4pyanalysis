import time
import uuid
from datetime import datetime
import networkx as nx
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

class NetworkXJourneyManager(JourneyManager):
    def __init__(self):
        self.graph = nx.Graph()
        self.events = {}
        self.journeys = {}
        self.event_to_journey = {}
        
    def setup(self):
        pass
    
    def clean(self):
        self.graph.clear()
        self.events.clear()
        self.journeys.clear()
        self.event_to_journey.clear()
    
    def ingest_batch(self, events_batch):
        if not events_batch:
            return
        
        for ev in events_batch:
            event_id = ev['id']
            self.events[event_id] = {
                'id': event_id,
                'correlation_ids': ev['correlation_ids'],
                'status': 'NEW',
                'created_at': datetime.now().isoformat(),
                'payload': ev.get('payload', {})
            }
            
            # Add event node
            self.graph.add_node(event_id, type='event')
            
            # Add edges between events with shared correlation IDs
            for other_id, other_ev in self.events.items():
                if other_id != event_id:
                    shared = set(ev['correlation_ids']) & set(other_ev['correlation_ids'])
                    if shared:
                        self.graph.add_edge(event_id, other_id)
    
    def process_events(self):
        # Get all NEW events
        new_event_ids = [eid for eid, ev in self.events.items() if ev['status'] == 'NEW']
        
        if not new_event_ids:
            return
        
        # Find connected components
        new_subgraph = self.graph.subgraph(new_event_ids)
        clusters = list(nx.connected_components(new_subgraph))
        
        for cluster in clusters:
            cluster_event_ids = list(cluster)
            
            # Collect all correlation IDs
            all_cids = set()
            for eid in cluster_event_ids:
                all_cids.update(self.events[eid]['correlation_ids'])
            
            # Find existing journeys
            existing_journey_ids = set()
            for eid in self.events:
                if self.events[eid]['status'] == 'PROCESSED':
                    if eid in self.event_to_journey:
                        shared = set(self.events[eid]['correlation_ids']) & all_cids
                        if shared:
                            existing_journey_ids.add(self.event_to_journey[eid])
            
            existing_journeys = sorted(
                [{'id': jid, 'created_at': self.journeys[jid]['created_at']} for jid in existing_journey_ids],
                key=lambda x: x['created_at']
            )
            
            if not existing_journeys:
                # Create new journey
                journey_id = f"journey_{uuid.uuid4()}"
                self.journeys[journey_id] = {
                    'id': journey_id,
                    'created_at': datetime.now().isoformat()
                }
                
            elif len(existing_journeys) == 1:
                journey_id = existing_journeys[0]['id']
                
            else:
                # Merge journeys
                journey_id = existing_journeys[0]['id']
                loser_ids = [j['id'] for j in existing_journeys[1:]]
                
                # Move events from loser journeys to winner
                for eid, jid in list(self.event_to_journey.items()):
                    if jid in loser_ids:
                        self.event_to_journey[eid] = journey_id
                
                # Delete loser journeys
                for jid in loser_ids:
                    del self.journeys[jid]
            
            # Link events to journey
            for eid in cluster_event_ids:
                self.event_to_journey[eid] = journey_id
                self.events[eid]['status'] = 'PROCESSED'
    
    def get_journey(self, event_id):
        if event_id not in self.event_to_journey:
            return None
        
        journey_id = self.event_to_journey[event_id]
        event_ids = [eid for eid, jid in self.event_to_journey.items() if jid == journey_id]
        
        return {
            "journey_id": journey_id,
            "events": event_ids
        }

if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching
    
    jm = NetworkXJourneyManager()
    jm.setup()
    jm.clean()
    
    NUM_JOURNEYS = 1000
    EVENTS_PER_JOURNEY = 5
    
    print("Starting Benchmark: NetworkX")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_JOURNEY)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)
