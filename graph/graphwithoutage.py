import psycopg2
from psycopg2.extras import execute_values, Json
import uuid
import time
import logging
import colorlog
import concurrent.futures
import networkx as nx
import random
from io import StringIO

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": "localhost",
    "database": "journey_db",
    "user": "admin",
    "password": "password",
    "port": "5432"
}
BATCH_SIZE = 50000
WORKERS = 4

# --- LOGGING ---
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    log_colors={'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red'}
))
logger = colorlog.getLogger('LookupPipeline')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class Timer:
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.start = time.time()
        logger.info(f"⏱️  START: {self.name}")
    def __exit__(self, *args):
        dur = time.time() - self.start
        logger.info(f"✅ END:   {self.name} took {dur:.4f}s")

class LookupOptimizedPipeline:
    
    def get_conn(self):
        return psycopg2.connect(**DB_CONFIG)

    def init_schema(self):
        with Timer("Schema Init (With Lookup Table)"):
            conn = self.get_conn()
            conn.autocommit = True
            cur = conn.cursor()
            
            # 1. Staging
            cur.execute("DROP TABLE IF EXISTS events_staging;")
            cur.execute("CREATE UNLOGGED TABLE events_staging (event_id UUID, app_name TEXT, correlation_keys TEXT[], payload JSONB);")
            
            # 2. Main Tables
            cur.execute("DROP TABLE IF EXISTS journey_keys CASCADE;") # <--- NEW OPTIMIZATION TABLE
            cur.execute("DROP TABLE IF EXISTS events CASCADE;")
            cur.execute("DROP TABLE IF EXISTS journeys CASCADE;")
            
            cur.execute("CREATE TABLE journeys (journey_id UUID PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW());")
            
            # 3. THE LOOKUP TABLE (Your Idea)
            # This table maps specific keys to journey IDs. It is much smaller and faster than 'events'.
            cur.execute("""
                CREATE TABLE journey_keys (
                    key_val TEXT PRIMARY KEY, -- Fast B-Tree Lookup
                    journey_id UUID REFERENCES journeys(journey_id) ON DELETE CASCADE
                );
            """)

            cur.execute("""
                CREATE TABLE events (
                    event_id UUID PRIMARY KEY,
                    app_name TEXT,
                    correlation_keys TEXT[],
                    payload JSONB,
                    journey_id UUID REFERENCES journeys(journey_id),
                    status VARCHAR(20) DEFAULT 'PENDING',
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            cur.execute("CREATE INDEX idx_keys ON events USING GIN (correlation_keys);")
            conn.close()

    def ingest_worker(self, chunk):
        conn = self.get_conn()
        try:
            q = "INSERT INTO events_staging (event_id, app_name, correlation_keys, payload) VALUES %s"
            data = [(e['id'], e['app'], e['keys'], Json(e.get('pl', {}))) for e in chunk]
            with conn.cursor() as cur:
                execute_values(cur, q, data)
            conn.commit()
        finally:
            conn.close()

    # ===========================================================
    #  THE OPTIMIZED STITCHING LOGIC
    # ===========================================================
    def run_stitching(self):
        conn = self.get_conn()
        conn.autocommit = False
        
        try:
            # 1. FETCH PENDING DATA
            with Timer("Fetching Pending Data"):
                with conn.cursor() as cur:
                    cur.execute("SELECT event_id, correlation_keys FROM events_staging")
                    staging_rows = cur.fetchall()
            
            if not staging_rows: return

            # 2. FETCH EXISTING JOURNEYS VIA LOOKUP TABLE (FAST!)
            # Instead of scanning the massive 'events' table, we scan the tiny 'journey_keys' table.
            
            # Get all unique keys in this batch
            batch_keys_set = set()
            for _, keys in staging_rows:
                batch_keys_set.update(keys)
            
            with Timer("Fetching Overlaps (Lookup Table)"):
                with conn.cursor() as cur:
                    # Fast lookup: Give me JIDs for these keys
                    cur.execute("CREATE TEMP TABLE batch_keys_temp (k text)")
                    # Use COPY for speed
                    keys_io = StringIO("\n".join(batch_keys_set))
                    cur.copy_from(keys_io, 'batch_keys_temp', columns=('k',))
                    
                    cur.execute("""
                        SELECT key_val, journey_id 
                        FROM journey_keys 
                        WHERE key_val IN (SELECT k FROM batch_keys_temp)
                    """)
                    existing_links = cur.fetchall() # List of (key, jid)

            # 3. IN-MEMORY GRAPH (NetworkX)
            with Timer("Resolving Logic (NetworkX)"):
                G = nx.Graph()
                
                # Add Staging Events: (EventID) <-> (Key)
                for eid, keys in staging_rows:
                    eid_str = str(eid)
                    G.add_node(eid_str, type='event')
                    for k in keys:
                        G.add_node(k, type='key')
                        G.add_edge(eid_str, k)
                        
                # Add Existing Journey Links: (Key) <-> (JourneyID)
                # This connects new events to old journeys
                for key_val, jid in existing_links:
                    jid_str = str(jid)
                    G.add_node(jid_str, type='journey')
                    G.add_edge(key_val, jid_str)

                components = list(nx.connected_components(G))

            # 4. CALCULATE UPDATES & NEW KEYS
            with Timer("Calculating Updates"):
                new_journeys_io = StringIO()
                event_updates_io = StringIO()
                merge_updates_io = StringIO()
                
                # We also need to update the Lookup Table with new keys!
                new_lookup_keys = [] # (key, jid)
                
                journeys_to_delete = set()
                
                for comp in components:
                    c_events = [n for n in comp if G.nodes[n].get('type') == 'event']
                    c_journeys = [n for n in comp if G.nodes[n].get('type') == 'journey']
                    c_keys = [n for n in comp if G.nodes[n].get('type') == 'key']
                    
                    if not c_events and not c_journeys: continue
                    
                    final_jid = None

                    # Logic: New vs Existing vs Merge
                    if not c_journeys:
                        final_jid = str(uuid.uuid4())
                        new_journeys_io.write(f"{final_jid}\n")
                    elif len(c_journeys) == 1:
                        final_jid = c_journeys[0]
                    else:
                        # MERGE
                        c_journeys.sort() 
                        final_jid = c_journeys[0]
                        for loser in c_journeys[1:]:
                            journeys_to_delete.add(loser)
                            merge_updates_io.write(f"{final_jid}\t{loser}\n")
                    
                    # Assign events
                    for eid in c_events:
                        event_updates_io.write(f"{final_jid}\t{eid}\n")
                    
                    # IMPORTANT: Register ALL keys in this cluster to the Winner Journey
                    # This keeps our Lookup Table up to date
                    for k in c_keys:
                        new_lookup_keys.append((k, final_jid))

                new_journeys_io.seek(0)
                event_updates_io.seek(0)
                merge_updates_io.seek(0)

            # 5. BULK WRITE
            with Timer("Bulk DB Writes"):
                with conn.cursor() as cur:
                    # A. Create Journeys
                    cur.copy_from(new_journeys_io, 'journeys', columns=('journey_id',))
                    
                    # B. Create Resolution Table
                    cur.execute("CREATE TEMP TABLE event_resolution (jid UUID, eid UUID) ON COMMIT DROP")
                    cur.copy_from(event_updates_io, 'event_resolution', columns=('jid', 'eid'))
                    
                    # C. Move Staging -> Main
                    cur.execute("""
                        INSERT INTO events (event_id, app_name, correlation_keys, payload, journey_id)
                        SELECT s.event_id, s.app_name, s.correlation_keys, s.payload, r.jid
                        FROM events_staging s
                        JOIN event_resolution r ON s.event_id = r.eid
                    """)
                    
                    # D. UPDATE LOOKUP TABLE (UPSERT)
                    # We need to insert new keys. If key exists (and we merged), we update JID.
                    if new_lookup_keys:
                        # Use execute_values for the upsert
                        sql = """
                            INSERT INTO journey_keys (key_val, journey_id) 
                            VALUES %s 
                            ON CONFLICT (key_val) 
                            DO UPDATE SET journey_id = EXCLUDED.journey_id
                        """
                        execute_values(cur, sql, new_lookup_keys)

                    # E. Handle Merges (Cleanup)
                    if merge_updates_io.getvalue():
                        logger.warning(f"⚠️  MERGING {len(journeys_to_delete)} journeys...")
                        cur.execute("CREATE TEMP TABLE merge_map (winner_id UUID, loser_id UUID) ON COMMIT DROP")
                        cur.copy_from(merge_updates_io, 'merge_map', columns=('winner_id', 'loser_id'))
                        
                        # Update Main Events
                        cur.execute("""
                            UPDATE events e SET journey_id = m.winner_id
                            FROM merge_map m WHERE e.journey_id = m.loser_id
                        """)
                        
                        # Lookup table updates handled by ON CONFLICT above or Cascade?
                        # Better to be explicit for keys belonging to losers that weren't in this batch
                        cur.execute("""
                            UPDATE journey_keys jk SET journey_id = m.winner_id
                            FROM merge_map m WHERE jk.journey_id = m.loser_id
                        """)
                        
                        cur.execute("DELETE FROM journeys WHERE journey_id = ANY(%s::uuid[])", (list(journeys_to_delete),))
                
                # Clear Staging
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE events_staging")
            
            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Stitching Failed: {e}")
            raise
        finally:
            conn.close()

    def verify_lookup_integrity(self):
        """Verifies that the Lookup Table matches the Events Table."""
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM journey_keys")
            keys_count = cur.fetchone()[0]
            
            cur.execute("SELECT count(*) FROM journeys")
            j_count = cur.fetchone()[0]
            
        print("\n" + "="*50)
        print(f"📊 LOOKUP TABLE STATS")
        print(f"   Unique Keys Indexed: {keys_count}")
        print(f"   Total Journeys:      {j_count}")
        print("="*50 + "\n")

# --- GENERATOR ---
def generate_data(n_journeys):
    logger.info(f"🏭 Generating {n_journeys} clusters...")
    for i in range(n_journeys):
        e = f"u{i}@x.com"
        p = f"555-{i}"
        # Generate events
        yield {"id": str(uuid.uuid4()), "app": "Web", "keys": [f"e:{e}"], "pl": {}}
        yield {"id": str(uuid.uuid4()), "app": "Mob", "keys": [f"p:{p}"], "pl": {}}
        yield {"id": str(uuid.uuid4()), "app": "Back", "keys": [f"e:{e}", f"p:{p}"], "pl": {}}

def main():
    pipe = LookupOptimizedPipeline()
    pipe.init_schema()
    
    TARGET = 5000 # 15,000 Events
    data = list(generate_data(TARGET))
    
    # Ingest
    chunks = [data[i:i+5000] for i in range(0, len(data), 5000)]
    with Timer("Total Ingestion"):
        with concurrent.futures.ProcessPoolExecutor(max_workers=WORKERS) as exe:
            exe.map(pipe.ingest_worker, chunks)
    
    # Stitch
    pipe.run_stitching()
    
    # Verify
    pipe.verify_lookup_integrity()

if __name__ == "__main__":
    main()