import psycopg2
from psycopg2.extras import execute_values, Json
from neo4j import GraphDatabase
import uuid
import time
import logging
import colorlog
import concurrent.futures
import random
from io import StringIO 

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
PG_CONFIG = {
    "host": "localhost",
    "database": "journey_db",
    "user": "admin",
    "password": "password",
    "port": "5432"
}
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password")

BATCH_SIZE = 5000
WORKERS = 4

# Logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    log_colors={'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red'}
))
logger = colorlog.getLogger('HeavyPipeline')
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

# ==============================================================================
# 2. PIPELINE LOGIC
# ==============================================================================
class Neo4jIncrementalPipeline:
    
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    def get_pg_conn(self):
        return psycopg2.connect(**PG_CONFIG)

    def close(self):
        self.driver.close()

    def init_db(self):
        """Sets up the environment."""
        with Timer("Infrastructure Init"):
            conn = self.get_pg_conn()
            conn.autocommit = True
            cur = conn.cursor()
            
            # Postgres Tables
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
                    status VARCHAR(20) DEFAULT 'PENDING', 
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Indexes
            cur.execute("CREATE INDEX idx_keys ON events USING GIN (correlation_keys);")
            cur.execute("CREATE INDEX idx_status ON events(status);") 
            cur.execute("CREATE INDEX idx_jid ON events(journey_id);") 
            conn.close()

            # Neo4j Constraints
            try:
                with self.driver.session() as session:
                    session.run("MATCH (n) DETACH DELETE n") 
                    try: session.run("CREATE CONSTRAINT FOR (e:Event) REQUIRE e.id IS UNIQUE") 
                    except: pass
                    try: session.run("CREATE CONSTRAINT FOR (k:Key) REQUIRE k.val IS UNIQUE") 
                    except: pass
            except Exception as e:
                logger.error(f"Neo4j Init Failed: {e}")
                raise

    # --------------------------------------------------------------------------
    # STEP 1: INGESTION
    # --------------------------------------------------------------------------
    def ingest_worker(self, chunk):
        conn = self.get_pg_conn()
        try:
            q = "INSERT INTO events_staging (event_id, app_name, correlation_keys, payload) VALUES %s"
            data = [(e['id'], e['app'], e['keys'], Json(e.get('pl', {}))) for e in chunk]
            with conn.cursor() as cur:
                execute_values(cur, q, data)
            conn.commit()
        finally:
            conn.close()

    def move_staging_to_main(self):
        conn = self.get_pg_conn()
        conn.autocommit = True
        with Timer("Moving Staging -> Main"):
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM events_staging")
                cnt = cur.fetchone()[0]
                if cnt == 0: return False
                
                cur.execute("""
                    INSERT INTO events (event_id, app_name, correlation_keys, payload)
                    SELECT event_id, app_name, correlation_keys, payload FROM events_staging
                """)
                cur.execute("TRUNCATE events_staging")
        conn.close()
        return True

    # --------------------------------------------------------------------------
    # STEP 2: GRAPH SYNC
    # --------------------------------------------------------------------------
    def sync_to_neo4j(self):
        conn = self.get_pg_conn()
        conn.autocommit = False
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT event_id, correlation_keys FROM events WHERE status = 'PENDING' LIMIT %s", (BATCH_SIZE * 2,))
                rows = cur.fetchall()
            
            if not rows: return False

            batch_data = []
            for eid, keys in rows:
                clean_keys = [k for k in keys]
                batch_data.append({'id': str(eid), 'keys': clean_keys})

            cypher = """
            UNWIND $batch AS row
            MERGE (e:Event {id: row.id})
            WITH e, row
            UNWIND row.keys AS key_val
            MERGE (k:Key {val: key_val})
            MERGE (e)-[:HAS_KEY]->(k)
            """

            chunks = [batch_data[i:i+BATCH_SIZE] for i in range(0, len(batch_data), BATCH_SIZE)]
            
            with Timer(f"Syncing {len(rows)} New Events to Graph"):
                with self.driver.session() as session:
                    for chunk in chunks:
                        session.run(cypher, batch=chunk)

            # Mark as Synced
            ids = [r['id'] for r in batch_data]
            with conn.cursor() as cur:
                cur.execute("UPDATE events SET status = 'GRAPH_SYNCED' WHERE event_id = ANY(%s::uuid[])", (ids,))
            
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Sync Failed: {e}")
            raise
        finally:
            conn.close()

    # --------------------------------------------------------------------------
    # STEP 3: STITCHING
    # --------------------------------------------------------------------------
    def run_incremental_stitching(self):
        conn = self.get_pg_conn()
        conn.autocommit = False
        
        try:
            with conn.cursor() as cur:
                # Get Batch
                cur.execute("SELECT event_id FROM events WHERE status = 'GRAPH_SYNCED' LIMIT 5000")
                target_ids = [str(r[0]) for r in cur.fetchall()]
                
                if not target_ids: return False

                processed_ids = set()
                new_journeys_io = StringIO()
                update_map_io = StringIO()
                merge_map_io = StringIO()
                journeys_to_delete = set()

                with Timer(f"Stitching Batch ({len(target_ids)} new events)"):
                    with self.driver.session() as session:
                        for eid in target_ids:
                            if eid in processed_ids: continue

                            # Local Traversal
                            result = session.run("""
                                MATCH (start:Event {id: $eid})-[:HAS_KEY*..4]-(connected:Event)
                                RETURN connected.id AS id
                            """, eid=eid)
                            
                            cluster = {eid}
                            for record in result:
                                cluster.add(record["id"])
                            
                            processed_ids.update(cluster)
                            
                            # Resolution
                            cluster_list = list(cluster)
                            cur.execute("""
                                SELECT DISTINCT journey_id 
                                FROM events 
                                WHERE event_id = ANY(%s::uuid[]) 
                                AND journey_id IS NOT NULL
                            """, (cluster_list,))
                            
                            existing_jids = [str(r[0]) for r in cur.fetchall()]
                            
                            final_jid = None
                            
                            if not existing_jids:
                                final_jid = str(uuid.uuid4())
                                new_journeys_io.write(f"{final_jid}\n")
                            elif len(existing_jids) == 1:
                                final_jid = existing_jids[0]
                            else:
                                final_jid = existing_jids[0]
                                losers = existing_jids[1:]
                                for loser in losers:
                                    if loser not in journeys_to_delete:
                                        journeys_to_delete.add(loser)
                                        merge_map_io.write(f"{final_jid}\t{loser}\n")
                            
                            for member_id in cluster_list:
                                update_map_io.write(f"{final_jid}\t{member_id}\n")

                    # Reset buffers
                    new_journeys_io.seek(0)
                    update_map_io.seek(0)
                    merge_map_io.seek(0)

                # DB Updates
                with Timer("Applying DB Updates"):
                    if new_journeys_io.getvalue():
                        cur.copy_from(new_journeys_io, 'journeys', columns=('journey_id',))
                    
                    if update_map_io.getvalue():
                        cur.execute("CREATE TEMP TABLE updates (jid UUID, eid UUID) ON COMMIT DROP")
                        cur.copy_from(update_map_io, 'updates', columns=('jid', 'eid'))
                        
                        cur.execute("""
                            UPDATE events e 
                            SET journey_id = u.jid, status = 'STITCHED'
                            FROM updates u
                            WHERE e.event_id = u.eid
                        """)
                    
                    if merge_map_io.getvalue():
                        cur.execute("CREATE TEMP TABLE merges (wid UUID, lid UUID) ON COMMIT DROP")
                        cur.copy_from(merge_map_io, 'merges', columns=('wid', 'lid'))
                        
                        cur.execute("UPDATE events e SET journey_id = m.wid FROM merges m WHERE e.journey_id = m.lid")
                        cur.execute("DELETE FROM journeys WHERE journey_id = ANY(%s::uuid[])", (list(journeys_to_delete),))

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Stitching Failed: {e}")
            raise
        finally:
            conn.close()

    def verify(self, target):
        conn = self.get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM journeys")
            cnt = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM events")
            ec = cur.fetchone()[0]
            
            avg = ec / cnt if cnt > 0 else 0
            
        print(f"\n📊 Final: {ec} Events | {cnt} Journeys (Target: {target})")
        print(f"   Average Events per Journey: {avg:.1f} (Target: ~180)")
        conn.close()

# ==============================================================================
# 3. UPDATED GENERATOR (50-70 Events Per App)
# ==============================================================================
def generate_phased_data(n_journeys, min_events=50, max_events=70):
    logger.info(f"🏭 Generating Phase 1: Web & Mobile ({min_events}-{max_events} events per app)...")
    phase1 = []
    for i in range(n_journeys):
        e = f"u{i}@x.com"
        p = f"555-{i}"
        
        # Generate Range for Web
        for _ in range(random.randint(min_events, max_events)):
            phase1.append({"id": str(uuid.uuid4()), "app": "Web", "keys": [f"e:{e}"], "pl": {}})
            
        # Generate Range for Mobile
        for _ in range(random.randint(min_events, max_events)):
            phase1.append({"id": str(uuid.uuid4()), "app": "Mob", "keys": [f"p:{p}"], "pl": {}})
    
    logger.info(f"🏭 Generating Phase 2: Backend Bridges ({min_events}-{max_events} events per app)...")
    phase2 = []
    for i in range(n_journeys):
        e = f"u{i}@x.com"
        p = f"555-{i}"
        
        # Generate Range for Backend
        for _ in range(random.randint(min_events, max_events)):
            phase2.append({"id": str(uuid.uuid4()), "app": "Back", "keys": [f"e:{e}", f"p:{p}"], "pl": {}})
        
    return phase1, phase2

# ==============================================================================
# 4. MAIN
# ==============================================================================
def main():
    pipe = Neo4jIncrementalPipeline()
    start_total = time.time()
    
    try:
        pipe.init_db()
        
        TARGET = 2000
        # Generates heavy load: 2000 * 3 apps * ~60 events = ~360,000 events
        p1, p2 = generate_phased_data(TARGET, min_events=50, max_events=70)
        
        # --- RUN PHASE 1 ---
        logger.info(f">>> PROCESSING PHASE 1 ({len(p1)} events)")
        
        # Ingest
        chunks = [p1[i:i+10000] for i in range(0, len(p1), 10000)] # Bigger ingest chunks for speed
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as exe:
            futures = [exe.submit(pipe.ingest_worker, c) for c in chunks]
            for f in concurrent.futures.as_completed(futures): f.result()
        
        pipe.move_staging_to_main()
        
        while True:
            has_new_graph = pipe.sync_to_neo4j()
            has_stitching = pipe.run_incremental_stitching()
            if not has_new_graph and not has_stitching: break

        # --- RUN PHASE 2 ---
        logger.info(f">>> PROCESSING PHASE 2 ({len(p2)} events)")
        
        chunks = [p2[i:i+10000] for i in range(0, len(p2), 10000)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as exe:
            futures = [exe.submit(pipe.ingest_worker, c) for c in chunks]
            for f in concurrent.futures.as_completed(futures): f.result()
            
        pipe.move_staging_to_main()
        
        while True:
            has_new_graph = pipe.sync_to_neo4j()
            has_stitching = pipe.run_incremental_stitching()
            if not has_new_graph and not has_stitching: break
        
        pipe.verify(TARGET)
        
    finally:
        end_total = time.time()
        logger.info(f"🏁 TOTAL EXECUTION TIME: {end_total - start_total:.2f}s")
        pipe.close()

if __name__ == "__main__":
    main()