import sys
import os
import uuid
import time
import logging
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from cassandra.concurrent import execute_concurrent

# Add parent directory to path to import common modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from common.interface import JourneyManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CassandraJourneyManager(JourneyManager):
    def __init__(self, contact_points=['127.0.0.1'], port=9042):
        self.cluster = Cluster(contact_points, port=port)
        self.session = self.cluster.connect()
        self.keyspace = "journey_stitching"

    def setup(self):
        logger.info("Setting up Cassandra schema...")
        self.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        self.session.set_keyspace(self.keyspace)

        # Table to store events
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id text PRIMARY KEY,
                journey_id text,
                correlation_ids set<text>,
                payload text,
                created_at timestamp
            )
        """)

        # Table to map correlation IDs to Journey IDs
        # We use this to find if a journey exists for a given key
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS correlation_mapping (
                correlation_id text PRIMARY KEY,
                journey_id text
            )
        """)

        # Table to store journey details (reverse lookup for merging)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS journeys (
                journey_id text PRIMARY KEY,
                correlation_ids set<text>,
                event_ids set<text>,
                created_at timestamp
            )
        """)
        
        # Prepare statements
        self.prep_insert_event = self.session.prepare("INSERT INTO events (event_id, journey_id, correlation_ids, payload, created_at) VALUES (?, ?, ?, ?, toTimestamp(now()))")
        self.prep_select_mapping = self.session.prepare("SELECT journey_id FROM correlation_mapping WHERE correlation_id = ?")
        self.prep_insert_mapping_lwt = self.session.prepare("INSERT INTO correlation_mapping (correlation_id, journey_id) VALUES (?, ?) IF NOT EXISTS")
        self.prep_update_mapping = self.session.prepare("UPDATE correlation_mapping SET journey_id = ? WHERE correlation_id = ?")
        
        self.prep_insert_journey = self.session.prepare("INSERT INTO journeys (journey_id, correlation_ids, event_ids, created_at) VALUES (?, ?, ?, toTimestamp(now()))")
        self.prep_select_journey = self.session.prepare("SELECT * FROM journeys WHERE journey_id = ?")
        self.prep_update_journey_events = self.session.prepare("UPDATE journeys SET event_ids = event_ids + ? WHERE journey_id = ?")
        self.prep_update_journey_correlations = self.session.prepare("UPDATE journeys SET correlation_ids = correlation_ids + ? WHERE journey_id = ?")
        self.prep_delete_journey = self.session.prepare("DELETE FROM journeys WHERE journey_id = ?")

    def clean(self):
        logger.info("Cleaning Cassandra data...")
        self.session.set_keyspace(self.keyspace)
        tables = ['events', 'correlation_mapping', 'journeys']
        for table in tables:
            try:
                self.session.execute(f"TRUNCATE {table}")
            except Exception as e:
                logger.warning(f"Could not truncate {table}: {e}")

    def ingest_batch(self, events_batch):
        """
        Ingest and stitch events.
        For high volume, we might want to separate ingest and stitching, 
        but for this requirement we stitch on ingest or immediately after.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Use a thread pool to process events in parallel
        # Be careful with race conditions - our logic handles them via LWT/locking
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self._process_single_event, event) for event in events_batch]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing event: {e}")

    def _process_single_event(self, event):
        event_id = event['id']
        correlation_ids = set(event['correlation_ids'])
        payload = str(event['payload'])
        
        # 1. Find existing journeys for these correlation IDs
        existing_journey_ids = set()
        
        # We need to query for each correlation ID
        # In a real high-perf scenario, we'd run these in parallel
        futures = []
        for cid in correlation_ids:
            futures.append((cid, self.session.execute_async(self.prep_select_mapping, [cid])))
        
        for cid, future in futures:
            rows = future.result()
            for row in rows:
                existing_journey_ids.add(row.journey_id)
        
        final_journey_id = None
        
        if not existing_journey_ids:
            # Case 0: New Journey
            new_jid = str(uuid.uuid4())
            # Try to claim all keys
            if self._claim_keys(correlation_ids, new_jid):
                final_journey_id = new_jid
                # Create journey record
                self.session.execute(self.prep_insert_journey, [final_journey_id, correlation_ids, {event_id}])
            else:
                # Race condition! Someone claimed a key. Retry this event.
                # Recursive retry (simple backoff could be added)
                return self._process_single_event(event)
                
        elif len(existing_journey_ids) == 1:
            # Case 1: Add to existing
            final_journey_id = list(existing_journey_ids)[0]
            # Add any new correlation keys to this journey
            self._claim_keys(correlation_ids, final_journey_id, force=True) # Force update if they point to nothing, but they might point to this JID already
            # Update journey record
            self.session.execute(self.prep_update_journey_events, [{event_id}, final_journey_id])
            self.session.execute(self.prep_update_journey_correlations, [correlation_ids, final_journey_id])
            
        else:
            # Case 2: Merge
            # Pick winner (lexicographically first for determinism, or oldest if we tracked creation time)
            sorted_jids = sorted(list(existing_journey_ids))
            winner_jid = sorted_jids[0]
            loser_jids = sorted_jids[1:]
            
            self._merge_journeys(winner_jid, loser_jids)
            final_journey_id = winner_jid
            
            # Add current event
            self._claim_keys(correlation_ids, final_journey_id, force=True)
            self.session.execute(self.prep_update_journey_events, [{event_id}, final_journey_id])
            self.session.execute(self.prep_update_journey_correlations, [correlation_ids, final_journey_id])

        # Finally insert the event
        self.session.execute(self.prep_insert_event, [event_id, final_journey_id, correlation_ids, payload])

    def _claim_keys(self, correlation_ids, journey_id, force=False):
        """
        Try to map correlation_ids to journey_id.
        If force=False, use LWT to ensure no overwrite.
        If force=True, overwrite (used during merge or adding to existing).
        Returns True if successful (or if force=True).
        Returns False if LWT failed (race condition).
        """
        if force:
            batch = BatchStatement()
            for cid in correlation_ids:
                batch.add(self.prep_update_mapping, [journey_id, cid])
            self.session.execute(batch)
            return True
        else:
            # LWT for new keys
            # We can't batch LWT easily if they are on different partitions (correlation_id is PK)
            # So we do them one by one or in parallel.
            # If any fails, we have a conflict.
            
            # Optimistic approach: Try to insert all.
            # If any fails, we need to rollback? No, just return False and let the caller handle it (by checking who owns it).
            # Actually, if we partially claimed keys, we might leave dangling keys if we fail.
            # But since we retry the event, we will eventually resolve it.
            
            success = True
            for cid in correlation_ids:
                # IF NOT EXISTS
                rs = self.session.execute(self.prep_insert_mapping_lwt, [cid, journey_id])
                if not rs.was_applied:
                    # Check if it's already the same journey_id (idempotent)
                    row = rs.one()
                    if row and row.journey_id == journey_id:
                        continue
                    success = False
                    break
            
            if not success:
                # We failed to claim one. 
                # Ideally we should release the ones we claimed, but that's complex.
                # Since we will retry the event, the retry logic will see the keys we DID claim as "existing_journey_ids".
                # It will see the keys we FAILED to claim as "existing_journey_ids" (owned by someone else).
                # It will then trigger a MERGE between our partial JID and the other JID.
                # So eventually it converges.
                return False
            return True

    def _merge_journeys(self, winner_jid, loser_jids):
        logger.info(f"Merging journeys {loser_jids} into {winner_jid}")
        
        for loser_jid in loser_jids:
            # 1. Get loser details
            row = self.session.execute(self.prep_select_journey, [loser_jid]).one()
            if not row:
                continue
                
            loser_cids = row.correlation_ids
            loser_eids = row.event_ids
            
            if not loser_cids or not loser_eids:
                continue

            # 2. Update mapping for all loser correlations
            # We can do this in parallel or batch (if small enough)
            # Since correlation_id is partition key, batch is logged batch (slower but atomic-ish)
            # or just async updates.
            futures = []
            for cid in loser_cids:
                futures.append(self.session.execute_async(self.prep_update_mapping, [winner_jid, cid]))
            for f in futures:
                f.result()
            
            # 3. Update events table for all loser events
            # This is expensive if many events.
            # But required for "get_journey" to work by querying events.
            # Alternatively, we only rely on 'journeys' table for grouping.
            # But the user wants "clean data".
            futures = []
            for eid in loser_eids:
                # We need a prepared statement for this
                stmt = SimpleStatement(f"UPDATE events SET journey_id = '{winner_jid}' WHERE event_id = '{eid}'")
                futures.append(self.session.execute_async(stmt))
            for f in futures:
                f.result()

            # 4. Move data to winner in 'journeys' table
            self.session.execute(self.prep_update_journey_events, [loser_eids, winner_jid])
            self.session.execute(self.prep_update_journey_correlations, [loser_cids, winner_jid])
            
            # 5. Delete loser journey
            self.session.execute(self.prep_delete_journey, [loser_jid])

    def process_events(self):
        # In this implementation, processing happens at ingest.
        pass

    def get_journey(self, event_id):
        # Get event to find journey_id
        row = self.session.execute("SELECT journey_id FROM events WHERE event_id = %s", [event_id]).one()
        if not row:
            return None
        
        jid = row.journey_id
        
        # Get journey details
        j_row = self.session.execute(self.prep_select_journey, [jid]).one()
        if not j_row:
            return None
            
        return {
            'journey_id': jid,
            'events': list(j_row.event_ids),
            'correlation_ids': list(j_row.correlation_ids)
        }

if __name__ == "__main__":
    # Example usage for testing
    jm = CassandraJourneyManager()
    jm.setup()
    jm.clean()
    print("Cassandra Journey Manager initialized.")
