import psycopg2
from psycopg2.extras import execute_values, Json
import uuid
import time
import logging
import colorlog
import concurrent.futures
import networkx as nx # The Graph Logic Library
import random
from tabulate import tabulate

# --- 1. CONFIGURATION ---
# Standard Postgres connection settings.
DB_CONFIG = {
    "host": "localhost",
    "database": "journey_db",
    "user": "admin",
    "password": "password",
    "port": "5432"
}

# BATCH_SIZE: How many events we process in one go.
# Too small (e.g., 100) = Too much network overhead.
# Too large (e.g., 1M) = Python might run out of RAM.
# 20,000 is a "Sweet Spot" for most laptops.
BATCH_SIZE = 20000 
WORKERS = 4 # Number of parallel CPU threads for ingestion.

# --- 2. LOGGING SETUP ---
# Sets up pretty colored logs so you can easily see errors (Red) vs Info (Green).
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    log_colors={'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'DEBUG': 'cyan'}
))
logger = colorlog.getLogger('EduPipeline')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- 3. TIMER UTILITY ---
# A helper class to measure how long specific blocks of code take.
# Essential for performance tuning (knowing where the bottleneck is).
class Timer:
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.start = time.time()
        logger.info(f"⏱️  START: {self.name}")
    def __exit__(self, *args):
        dur = time.time() - self.start
        logger.info(f"✅ END:   {self.name} took {dur:.4f}s")

# --- 4. THE MAIN PIPELINE ---
class EducationalPipeline:
    
    def get_conn(self):
        """Creates a new connection to Postgres."""
        return psycopg2.connect(**DB_CONFIG)

    def init_db(self):
        """
        Resets the database to a clean state.
        Creates Tables and Indexes optimized for high-speed stitching.
        """
        with Timer("Database Initialization"):
            conn = self.get_conn()
            conn.autocommit = True # Auto-save changes (no manual commit needed here)
            cur = conn.cursor()
            
            # --- A. THE STAGING TABLE ---
            # We use an 'UNLOGGED' table for staging.
            # Standard tables write to a 'Write Ahead Log' (WAL) for crash safety.
            # UNLOGGED tables skip this, making writes 2x faster.
            # Risk: If DB crashes, this table empties. Safe for temp data.
            cur.execute("DROP TABLE IF EXISTS events_staging;")
            cur.execute("""
                CREATE UNLOGGED TABLE events_staging (
                    event_id UUID,
                    app_name TEXT,
                    correlation_keys TEXT[], -- Array of strings (e.g. ["email:a@b.com", "phone:123"])
                    payload JSONB
                );
            """)
            
            # --- B. THE MAIN TABLES ---
            cur.execute("DROP TABLE IF EXISTS events CASCADE;")
            cur.execute("DROP TABLE IF EXISTS journeys CASCADE;")
            
            # Journeys: The "Container" or "Folder" that holds related events.
            cur.execute("CREATE TABLE journeys (journey_id UUID PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW());")
            
            # Events: The actual data (clicks, logs, transactions).
            cur.execute("""
                CREATE TABLE events (
                    event_id UUID PRIMARY KEY,
                    app_name TEXT,
                    correlation_keys TEXT[],
                    payload JSONB,
                    journey_id UUID REFERENCES journeys(journey_id), -- Link to the parent journey
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # --- C. THE INDEXES (CRITICAL FOR PERFORMANCE) ---
            # GIN Index (Generalized Inverted Index):
            # This is magic for Arrays. It allows Postgres to instantly find
            # any row where array ['A','B'] overlaps with ['B','C'].
            # Without this, the DB has to scan every single row (too slow).
            cur.execute("CREATE INDEX idx_keys ON events USING GIN (correlation_keys);")
            
            # Standard index for fast lookups by Journey ID.
            cur.execute("CREATE INDEX idx_jid ON events (journey_id);")
            conn.close()

    def ingest_worker(self, chunk):
        """
        This function runs in parallel on multiple CPU cores.
        It takes a 'chunk' of events and bulk-inserts them into Staging.
        """
        conn = self.get_conn()
        try:
            # SQL Query to insert data
            q = "INSERT INTO events_staging (event_id, app_name, correlation_keys, payload) VALUES %s"
            
            # Convert dictionary objects into a list of tuples (required by psycopg2)
            data = [(e['id'], e['app'], e['keys'], Json(e.get('pl', {}))) for e in chunk]
            
            with conn.cursor() as cur:
                # execute_values is a 'Fast Path' insert.
                # Instead of running "INSERT..." 10,000 times, it sends one
                # massive data packet to Postgres. Much faster I/O.
                execute_values(cur, q, data)
            conn.commit()
        finally:
            conn.close()

    def process_staging_data(self):
        """
        THE BRAIN OF THE OPERATION.
        1. Reads raw events from Staging.
        2. Builds a Graph in memory to find connections.
        3. Writes the stitched results to the Main tables.
        """
        conn = self.get_conn()
        conn.autocommit = False # We want a Transaction (All or Nothing)
        try:
            # --- STEP 1: FETCH DATA ---
            # Load the raw data we just ingested into Python memory.
            with Timer("Fetch Staging Data"):
                with conn.cursor() as cur:
                    cur.execute("SELECT event_id, correlation_keys FROM events_staging")
                    staging_rows = cur.fetchall()
            
            if not staging_rows: return

            # --- STEP 2: IN-MEMORY STITCHING (GRAPH THEORY) ---
            # We use the NetworkX library to solve the "Connected Components" problem.
            # Imagine:
            #   Event A has key [Email_1]
            #   Event B has key [Phone_1]
            #   Event C has keys [Email_1, Phone_1]
            #
            # In the graph:
            #   (A) --connected to--> (Email_1) --connected to--> (C) --connected to--> (Phone_1) --connected to--> (B)
            #
            # Result: A, B, and C are all in the same "Component" (Cluster).
            
            with Timer(f"Stitching {len(staging_rows)} Events"):
                G = nx.Graph()
                staging_ids = set()
                
                for eid, keys in staging_rows:
                    eid = str(eid)
                    staging_ids.add(eid)
                    
                    # Add the Event as a Node
                    G.add_node(eid, type='event')
                    
                    for k in keys:
                        # Add the Key as a Node
                        G.add_node(k, type='key')
                        # Draw a line (Edge) between the Event and the Key
                        G.add_edge(eid, k)

                # This function finds the isolated clusters (The Journeys)
                # It returns a list of sets: [{A,B,C}, {D,E}, {F}]
                components = list(nx.connected_components(G))

            # --- STEP 3: PREPARE DATABASE UPDATES ---
            with Timer("Prepare Bulk Writes"):
                new_journeys = []
                event_updates = []
                
                for comp in components:
                    # Filter out the 'Key' nodes, keep only 'Event' nodes
                    c_events = [n for n in comp if G.nodes[n].get('type') == 'event']
                    if not c_events: continue
                    
                    # Generate a brand new Journey ID for this cluster
                    jid = str(uuid.uuid4())
                    new_journeys.append((jid,))
                    
                    # Link all events in this cluster to the new Journey ID
                    for eid in c_events:
                        if eid in staging_ids:
                            event_updates.append((jid, eid))

            # --- STEP 4: EXECUTE WRITES (BULK) ---
            with Timer("Commit to DB"):
                with conn.cursor() as cur:
                    # A. Create the Journey Containers
                    execute_values(cur, "INSERT INTO journeys (journey_id) VALUES %s", new_journeys)
                    
                    # B. Create a temporary mapping table in DB memory
                    # This helps us join the Staging data with our calculated Journey IDs
                    cur.execute("CREATE TEMP TABLE event_resolution (eid UUID, jid UUID)")
                    execute_values(cur, "INSERT INTO event_resolution (jid, eid) VALUES %s", event_updates)
                    
                    # C. Move data from Staging -> Events (Main)
                    # We JOIN staging with our resolution table to get the Journey ID
                    cur.execute("""
                        INSERT INTO events (event_id, app_name, correlation_keys, payload, journey_id)
                        SELECT s.event_id, s.app_name, s.correlation_keys, s.payload, r.jid
                        FROM events_staging s
                        JOIN event_resolution r ON s.event_id = r.eid
                    """)
                    
                    # D. Clean up staging for the next run
                    cur.execute("TRUNCATE events_staging")
                conn.commit() # Save changes permanently
                
        except Exception as e:
            conn.rollback() # Undo changes if error occurs
            logger.error(f"Error: {e}")
            raise
        finally:
            conn.close()

    # --- 5. VERIFICATION METHODS ---
    
    def inspect_random_samples(self, total_journeys, samples=10):
        """
        A Logic Test.
        Picks 10 random journeys and checks if they contain data from 
        ALL 3 APPS (Web, Mobile, Backend).
        If they do, stitching worked. If not, the data is fragmented.
        """
        logger.info(f"\n🎲 STARTING RANDOM SAMPLING TEST (Checking {samples} random journeys)...")
        conn = self.get_conn()
        
        try:
            with conn.cursor() as cur:
                for i in range(samples):
                    # Randomly pick a journey index
                    rand_offset = random.randint(0, total_journeys - 1)
                    
                    # Get the Journey ID at that index
                    cur.execute("SELECT journey_id FROM journeys OFFSET %s LIMIT 1", (rand_offset,))
                    res = cur.fetchone()
                    if not res: continue
                    jid = res[0]
                    
                    # Count how many events from each App exist in this journey
                    cur.execute("""
                        SELECT app_name, count(*) 
                        FROM events 
                        WHERE journey_id = %s 
                        GROUP BY app_name
                    """, (jid,))
                    counts = dict(cur.fetchall())
                    total_events = sum(counts.values())
                    
                    # Verification: Do we have 3 distinct apps?
                    apps_found = list(counts.keys())
                    is_success = len(apps_found) == 3 
                    status_icon = "✅" if is_success else "❌"
                    
                    print("\n" + "-"*60)
                    print(f"{status_icon} TEST {i+1}/{samples}: Journey #{rand_offset}")
                    print(f"   ID: {str(jid)}")
                    print(f"   Total Events: {total_events}")
                    print(f"   Breakdown:    {counts}")
                    
                    if not is_success:
                        logger.error(f"   FRAGMENTATION DETECTED! Only found: {apps_found}")
        finally:
            conn.close()
        print("-" * 60 + "\n")

    def verify_volume_stats(self, expected_journeys):
        """
        A Statistics Test.
        Checks total counts. If we generated 225,000 events and 
        expected 1,000 journeys, the DB should match exactly.
        """
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM journeys")
            j_count = cur.fetchone()[0]
            
            cur.execute("SELECT count(*) FROM events")
            e_count = cur.fetchone()[0]
            
            avg = e_count / j_count if j_count > 0 else 0
            
            # Count journeys that have fewer than 3 apps (Logic Failure)
            cur.execute("""
                SELECT count(*) FROM (
                    SELECT journey_id FROM events 
                    GROUP BY journey_id 
                    HAVING count(DISTINCT app_name) < 3
                ) as bad_journeys
            """)
            bad_count = cur.fetchone()[0]

        print("\n" + "="*40)
        print(f"📊 FINAL VOLUME STATISTICS")
        print(f"   Total Events:      {e_count}")
        print(f"   Journeys Created:  {j_count} (Target: {expected_journeys})")
        print(f"   Avg Journey Size:  {avg:.1f} events")
        print(f"   Fragmented Jrnys:  {bad_count} (Target: 0)")
        print("="*40)

# --- 6. DATA GENERATOR ---
def generate_heavy_traffic(num_journeys):
    """
    Generates 'Noise'.
    For every journey, it creates 50-100 events for Web, Mobile, and Backend.
    All share common keys (Email/Phone) so they *should* be stitched together.
    """
    logger.info(f"🏭 Generating {num_journeys} HEAVY journeys (approx {num_journeys * 225} total events)...")
    
    for i in range(num_journeys):
        # Unique Identifiers for this user
        email = f"u{i}@x.com"
        phone = f"555-{i}"
        
        # 1. Web Events (Knows Email)
        for _ in range(random.randint(50, 100)):
            yield {"id": str(uuid.uuid4()), "app": "Web_App", "keys": [f"email:{email}"], "pl": {}}
            
        # 2. Mobile Events (Knows Phone)
        for _ in range(random.randint(50, 100)):
            yield {"id": str(uuid.uuid4()), "app": "Mobile_App", "keys": [f"phone:{phone}"], "pl": {}}
            
        # 3. Backend Events (Knows BOTH -> The Bridge)
        for _ in range(random.randint(50, 100)):
            yield {"id": str(uuid.uuid4()), "app": "Backend", "keys": [f"email:{email}", f"phone:{phone}"], "pl": {}}

# --- 7. MAIN EXECUTION ---
def main():
    pipeline = EducationalPipeline()
    
    # A. Reset Database
    pipeline.init_db()
    
    # B. Configuration
    TARGET_JOURNEYS = 1000 # We want 1,000 unique user journeys
    
    # C. Data Generation (Lazy Evaluation for memory efficiency)
    data_iter = generate_heavy_traffic(TARGET_JOURNEYS)
    all_data = list(data_iter) # Materialize logic to list
    logger.info(f"📦 Generated {len(all_data)} events total.")
    
    # D. Parallel Ingestion
    # Split data into chunks of 20,000 for workers
    chunks = [all_data[i:i + BATCH_SIZE] for i in range(0, len(all_data), BATCH_SIZE)]
    
    with Timer("Parallel Ingestion Phase"):
        # Create a pool of 4 worker processes
        with concurrent.futures.ProcessPoolExecutor(max_workers=WORKERS) as executor:
            executor.map(pipeline.ingest_worker, chunks)
            
    # E. Stitching Phase
    pipeline.process_staging_data()
    
    # F. Final Verification
    pipeline.verify_volume_stats(TARGET_JOURNEYS)
    pipeline.inspect_random_samples(TARGET_JOURNEYS, samples=10)

if __name__ == "__main__":
    main()