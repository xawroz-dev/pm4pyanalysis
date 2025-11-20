import psycopg2
from psycopg2.extras import execute_values, Json
import uuid
import time
import logging
import colorlog
import concurrent.futures
import networkx as nx
from tabulate import tabulate

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": "localhost",
    "database": "journey_db",
    "user": "admin",
    "password": "password",
    "port": "5432"
}
BATCH_SIZE = 10000
WORKERS = 4 

# --- LOGGING ---
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    log_colors={'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red'}
))
logger = colorlog.getLogger('StrictPipeline')
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

class StrictPipeline:
    def get_conn(self):
        return psycopg2.connect(**DB_CONFIG)

    def init_db(self):
        with Timer("DB Init"):
            conn = self.get_conn()
            conn.autocommit = True
            cur = conn.cursor()
            
            # Reset everything
            cur.execute("DROP TABLE IF EXISTS events_staging;")
            cur.execute("CREATE UNLOGGED TABLE events_staging (event_id UUID, app_name TEXT, correlation_keys TEXT[], payload JSONB);")
            
            cur.execute("DROP TABLE IF EXISTS events CASCADE;")
            cur.execute("DROP TABLE IF EXISTS journeys CASCADE;")
            
            cur.execute("CREATE TABLE journeys (journey_id UUID PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW());")
            cur.execute("""
                CREATE TABLE events (
                    event_id UUID PRIMARY KEY,
                    app_name TEXT,
                    correlation_keys TEXT[],
                    payload JSONB,
                    journey_id UUID REFERENCES journeys(journey_id),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Indexes
            cur.execute("CREATE INDEX idx_keys ON events USING GIN (correlation_keys);")
            cur.execute("CREATE INDEX idx_jid ON events (journey_id);")
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

    def process_staging_data(self):
        conn = self.get_conn()
        conn.autocommit = False 
        try:
            # 1. Load Data
            with Timer("Fetch & Prep"):
                with conn.cursor() as cur:
                    cur.execute("SELECT event_id, correlation_keys FROM events_staging")
                    staging_rows = cur.fetchall()
            
            if not staging_rows: return

            # 2. In-Memory Stitching (NetworkX is fast enough for 300k nodes)
            with Timer("Union-Find Stitching"):
                G = nx.Graph()
                staging_ids = set()
                
                # Add Staging Nodes
                for eid, keys in staging_rows:
                    eid = str(eid)
                    staging_ids.add(eid)
                    G.add_node(eid, type='event')
                    for k in keys:
                        G.add_node(k, type='key')
                        G.add_edge(eid, k)

                # (Optional: Load Existing DB Nodes if this was an incremental run)
                # For this deterministic test, we assume clean slate or separate clusters.

                components = list(nx.connected_components(G))

            # 3. Prepare DB Updates
            with Timer("Prepare Bulk Writes"):
                new_journeys = []
                event_updates = []
                
                for comp in components:
                    # Only care about components containing events
                    c_events = [n for n in comp if G.nodes[n].get('type') == 'event']
                    if not c_events: continue
                    
                    # Create NEW Journey for this cluster
                    jid = str(uuid.uuid4())
                    new_journeys.append((jid,))
                    
                    for eid in c_events:
                        if eid in staging_ids:
                            event_updates.append((jid, eid))

            # 4. Execute Writes
            with Timer("Commit to DB"):
                with conn.cursor() as cur:
                    execute_values(cur, "INSERT INTO journeys (journey_id) VALUES %s", new_journeys)
                    
                    # Temp table for join update
                    cur.execute("CREATE TEMP TABLE event_resolution (eid UUID, jid UUID)")
                    execute_values(cur, "INSERT INTO event_resolution (jid, eid) VALUES %s", event_updates)
                    
                    cur.execute("""
                        INSERT INTO events (event_id, app_name, correlation_keys, payload, journey_id)
                        SELECT s.event_id, s.app_name, s.correlation_keys, s.payload, r.jid
                        FROM events_staging s
                        JOIN event_resolution r ON s.event_id = r.eid
                    """)
                    cur.execute("TRUNCATE events_staging")
                conn.commit()
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error: {e}")
            raise
        finally:
            conn.close()

    def verify_results(self, expected_journeys):
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM journeys")
            j_count = cur.fetchone()[0]
            
            cur.execute("SELECT count(*) FROM events")
            e_count = cur.fetchone()[0]
            
            # Check for split journeys (BAD)
            # We expect exactly 3 apps per journey in this dataset
            cur.execute("""
                SELECT count(*) FROM (
                    SELECT journey_id FROM events 
                    GROUP BY journey_id 
                    HAVING count(DISTINCT app_name) < 3
                ) as bad_journeys
            """)
            bad_count = cur.fetchone()[0]

        print("\n" + "="*40)
        print(f"📊 FINAL VERIFICATION")
        print(f"   Events Ingested:   {e_count} (Expected: {expected_journeys * 3})")
        print(f"   Journeys Created:  {j_count} (Expected: {expected_journeys})")
        print(f"   Incomplete Jrnys:  {bad_count} (Should be 0)")
        print("="*40)
        
        if j_count == expected_journeys and bad_count == 0:
            logger.info("✅ TEST PASSED: Perfect 1-to-1 mapping of clusters.")
        else:
            logger.error("❌ TEST FAILED: Stitching logic fragmented the data.")

    def print_sample_journey(self):
        conn = self.get_conn()
        with conn.cursor() as cur:
            # Pick one random journey
            cur.execute("SELECT journey_id FROM journeys LIMIT 1")
            jid = cur.fetchone()[0]
            
            cur.execute("SELECT app_name, correlation_keys FROM events WHERE journey_id = %s", (jid,))
            rows = cur.fetchall()
            
            print(f"\n🔎 INSPECTING SAMPLE JOURNEY: {jid}")
            print(tabulate(rows, headers=["App Name", "Keys"], tablefmt="grid"))

# --- THE DETERMINISTIC GENERATOR ---
def generate_structured_data(num_journeys):
    logger.info(f"🏭 Generating {num_journeys} deterministic journey clusters (3 events each)...")
    
    for i in range(num_journeys):
        # Shared IDs for this loop iteration
        email = f"user_{i}@test.com"
        phone = f"555-000-{i}"
        
        # Event 1: Web (Knows Email)
        yield {
            "id": str(uuid.uuid4()),
            "app": "Web_App",
            "keys": [f"email:{email}"],
            "pl": {"iter": i}
        }
        
        # Event 2: Mobile (Knows Phone)
        yield {
            "id": str(uuid.uuid4()),
            "app": "Mobile_App",
            "keys": [f"phone:{phone}"],
            "pl": {"iter": i}
        }
        
        # Event 3: Backend (The Bridge - Knows BOTH)
        # This guarantees 1 & 2 are stitched together
        yield {
            "id": str(uuid.uuid4()),
            "app": "Backend_System",
            "keys": [f"email:{email}", f"phone:{phone}"],
            "pl": {"iter": i}
        }

# --- MAIN ---
def main():
    pipeline = StrictPipeline()
    pipeline.init_db()
    
    # CONFIG: How many journeys do you want?
    TARGET_JOURNEYS = 10000  # This will result in 60,000 events
    
    # 1. Generate
    data_iter = generate_structured_data(TARGET_JOURNEYS)
    all_data = list(data_iter)
    
    # 2. Ingest
    chunks = [all_data[i:i + BATCH_SIZE] for i in range(0, len(all_data), BATCH_SIZE)]
    
    with Timer("Parallel Ingestion"):
        with concurrent.futures.ProcessPoolExecutor(max_workers=WORKERS) as executor:
            executor.map(pipeline.ingest_worker, chunks)
            
    # 3. Stitch
    pipeline.process_staging_data()
    
    # 4. Validate
    pipeline.verify_results(TARGET_JOURNEYS)
    pipeline.print_sample_journey()

if __name__ == "__main__":
    main()