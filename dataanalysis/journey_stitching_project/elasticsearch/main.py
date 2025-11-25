import sys
import os
import uuid
import time
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# Add parent directory to path to import common modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from common.interface import JourneyManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ElasticsearchJourneyManager(JourneyManager):
    def __init__(self, hosts=['http://localhost:9200']):
        self.es = Elasticsearch(hosts)
        self.index_name = "events"
        self.lock_index = "locks"

    def setup(self):
        logger.info("Setting up Elasticsearch indices...")
        
        # Events index
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body={
                "mappings": {
                    "properties": {
                        "event_id": {"type": "keyword"},
                        "journey_id": {"type": "keyword"},
                        "correlation_ids": {"type": "keyword"},
                        "payload": {"type": "object"}, # or text/keyword depending on need
                        "created_at": {"type": "date"}
                    }
                }
            })
            
        # Lock index for concurrency control
        if not self.es.indices.exists(index=self.lock_index):
            self.es.indices.create(index=self.lock_index, body={
                "mappings": {
                    "properties": {
                        "locked_at": {"type": "date"}
                    }
                }
            })

    def clean(self):
        logger.info("Cleaning Elasticsearch data...")
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
        if self.es.indices.exists(index=self.lock_index):
            self.es.indices.delete(index=self.lock_index)
        self.setup()

    def ingest_batch(self, events_batch):
        """
        Ingest and stitch events.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self._process_single_event, event) for event in events_batch]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing event: {e}")

    def _process_single_event(self, event):
        event_id = event['id']
        correlation_ids = list(set(event['correlation_ids']))
        
        # 1. Acquire Locks for all correlation IDs
        # This prevents other pods from processing events with the same keys simultaneously
        if not self._acquire_locks(correlation_ids):
            # Retry logic could be added here
            # For now, we just retry recursively with a small sleep
            time.sleep(0.1)
            return self._process_single_event(event)

        try:
            # 2. Search for existing journeys
            # We need to refresh to see recently indexed documents if we want strict consistency
            # self.es.indices.refresh(index=self.index_name) 
            # Refresh is expensive. In a real system, we might tolerate eventual consistency or use a separate consistent store for mapping.
            # But for "clean data" and "race condition" handling, we refresh or accept the lock overhead.
            # Since we have locks, we are safe from concurrent modifications, but we might not see data indexed by others yet if not refreshed.
            self.es.indices.refresh(index=self.index_name)

            query = {
                "bool": {
                    "should": [
                        {"terms": {"correlation_ids": correlation_ids}}
                    ],
                    "minimum_should_match": 1
                }
            }
            
            # We only need the journey_id
            resp = self.es.search(index=self.index_name, body={
                "query": query,
                "_source": ["journey_id"],
                "size": 1000 # Cap at some reasonable number
            })
            
            existing_journey_ids = set()
            for hit in resp['hits']['hits']:
                if 'journey_id' in hit['_source']:
                    existing_journey_ids.add(hit['_source']['journey_id'])
            
            final_journey_id = None
            
            if not existing_journey_ids:
                # New Journey
                final_journey_id = str(uuid.uuid4())
            elif len(existing_journey_ids) == 1:
                # Add to existing
                final_journey_id = list(existing_journey_ids)[0]
            else:
                # Merge
                sorted_jids = sorted(list(existing_journey_ids))
                winner_jid = sorted_jids[0]
                loser_jids = sorted_jids[1:]
                
                self._merge_journeys(winner_jid, loser_jids)
                final_journey_id = winner_jid
            
            # 3. Index the new event
            doc = {
                "event_id": event_id,
                "journey_id": final_journey_id,
                "correlation_ids": correlation_ids,
                "payload": event['payload'],
                "created_at": datetime.now().isoformat() if 'created_at' not in event else event['created_at']
            }
            # We use event_id as _id for idempotency
            self.es.index(index=self.index_name, id=event_id, body=doc)
            
        finally:
            # 4. Release Locks
            self._release_locks(correlation_ids)

    def _acquire_locks(self, keys):
        """
        Try to create a document for each key in the lock index.
        If any fail (409 Conflict), rollback and return False.
        """
        acquired = []
        try:
            for key in keys:
                self.es.create(index=self.lock_index, id=key, body={"locked_at": time.time()})
                acquired.append(key)
            return True
        except Exception as e:
            # Rollback
            self._release_locks(acquired)
            return False

    def _release_locks(self, keys):
        for key in keys:
            try:
                self.es.delete(index=self.lock_index, id=key)
            except:
                pass

    def _merge_journeys(self, winner_jid, loser_jids):
        logger.info(f"Merging journeys {loser_jids} into {winner_jid}")
        
        # Update by query
        # This can be long running.
        query = {
            "terms": {
                "journey_id": loser_jids
            }
        }
        
        script = {
            "source": "ctx._source.journey_id = params.winner_jid",
            "lang": "painless",
            "params": {
                "winner_jid": winner_jid
            }
        }
        
        self.es.update_by_query(index=self.index_name, body={
            "query": query,
            "script": script
        }, refresh=True)

    def process_events(self):
        pass

    def get_journey(self, event_id):
        # Get event
        try:
            resp = self.es.get(index=self.index_name, id=event_id)
            jid = resp['_source']['journey_id']
            
            # Get all events for this journey
            # Note: This might be large
            events_resp = self.es.search(index=self.index_name, body={
                "query": {"term": {"journey_id": jid}},
                "size": 10000
            })
            
            event_ids = [h['_source']['event_id'] for h in events_resp['hits']['hits']]
            
            return {
                'journey_id': jid,
                'events': event_ids
            }
        except Exception:
            return None

if __name__ == "__main__":
    jm = ElasticsearchJourneyManager()
    jm.setup()
    jm.clean()
    print("Elasticsearch Journey Manager initialized.")
