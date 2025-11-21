import psycopg2
from psycopg2.extras import execute_values, Json
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
DB_CONFIG = {
    "host": "localhost",
    "database": "journey_db",
    "user": "admin",
    "password": "password",
    "port": "5432"
}
BATCH_SIZE = 5000
GRAPH_NAME = "journey_graph"
WORKERS = 4 

# Logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    log_colors={'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red'}
))
logger = colorlog.getLogger('FixedPipeline')
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
class AgeNativePipeline:
    
    def get_conn(self):
        return psycopg2.connect(**DB_CONFIG)

    def init_db(self):
        """
        Fixed Initialization Order: Graph First, Then Tables.
        """
        with Timer("Infrastructure Initialization"):
            conn = self.get_conn()
            conn.autocommit = True
            cur = conn.cursor()
            
            # 1. Setup AGE
            cur.execute("CREATE EXTENSION IF NOT EXISTS age;")
            cur.execute("LOAD 'age';")
            cur.execute("SET search_path = ag_catalog, '$user', public;")
            
            # 2. Reset Graph (Safe Drop)
            try: cur.execute(f"SELECT drop_graph('{GRAPH_NAME}', true);")
            except: pass
            cur.execute(f"SELECT create_graph('{GRAPH_NAME}');")

            # 3. Reset Tables (Using public. prefix)
            cur.execute("DROP TABLE IF EXISTS public.events_staging;")
            cur.execute("CREATE UNLOGGED TABLE public.events_staging (event_id UUID, app_name TEXT, correlation_keys TEXT[], payload JSONB);")
            
            cur.execute("DROP TABLE IF EXISTS public.events CASCADE;")
            cur.execute("DROP TABLE IF EXISTS public.journeys CASCADE;")
            
            cur.execute("CREATE TABLE public.journeys (journey_id UUID PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW());")
            cur.execute("""
                CREATE TABLE public.events (
                    event_id UUID PRIMARY KEY,
                    app_name TEXT,
                    correlation_keys TEXT[],
                    payload JSONB,
                    journey_id UUID REFERENCES public.journeys(journey_id),
                    status VARCHAR(20) DEFAULT 'PENDING', 
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            cur.execute("CREATE INDEX idx_keys ON public.events USING GIN (correlation_keys);")
            cur.execute("CREATE INDEX idx_status ON public.events(status);")
            
            conn.close()

    def ingest_worker(self, chunk):
        conn = self.get_conn()
        try:
            q = "INSERT INTO public.events_staging (event_id, app_name, correlation_keys, payload) VALUES %s"
            data = [(e['id'], e['app'], e['keys'], Json(e.get('pl', {}))) for e in chunk]
            with conn.cursor() as cur:
                execute_values(cur, q, data)
            conn.commit()
        finally:
            conn.close()

    def move_staging_to_main(self):
        conn = self.get_conn()
        conn.autocommit = True
        with Timer("Moving Staging -> Main"):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.events (event_id, app_name, correlation_keys, payload)
                    SELECT event_id, app_name, correlation_keys, payload FROM public.events_staging
                """)
                cur.execute("TRUNCATE public.events_staging")
        conn.close()

    # --------------------------------------------------------------------------
    # STEP 2: GRAPH PROJECTION (SYNTAX FIXED)
    # --------------------------------------------------------------------------
    def project_to_graph(self):
        conn = self.get_conn()
        conn.autocommit = False
        
        try:
            # A. Fetch Pending
            with conn.cursor() as cur:
                cur.execute("SELECT event_id, correlation_keys FROM public.events WHERE status = 'PENDING'")
                rows = cur.fetchall()
            
            if not rows: return

            # B. Extract Unique Keys
            unique_keys = set()
            edge_list = []
            
            for eid, keys in rows:
                eid_str = str(eid)
                for k in keys:
                    k_clean = k.replace("'", "").replace('"', '') # Sanitize quotes
                    unique_keys.add(k_clean)
                    # We format the map string manually here to avoid JSON quoting issues
                    # Result: {id: '123', key: 'abc'}
                    edge_list.append(f"{{id: '{eid_str}', key: '{k_clean}'}}")

            # C. Load Keys (Nodes)
            key_list = list(unique_keys)
            key_batches = [key_list[i:i+BATCH_SIZE] for i in range(0, len(key_list), BATCH_SIZE)]
            
            with Timer(f"Loading {len(unique_keys)} Key Nodes"):
                with conn.cursor() as cur:
                    cur.execute("LOAD 'age'; SET search_path = ag_catalog, '$user', public;")
                    for batch in key_batches:
                        # FIXED: Manual string building. 
                        # Result: [{v: 'val1'}, {v: 'val2'}]
                        batch_str = "[" + ", ".join([f"{{v: '{k}'}}" for k in batch]) + "]"
                        
                        q = f"""
                            SELECT * FROM cypher('{GRAPH_NAME}', $$
                                WITH {batch_str} AS batch
                                UNWIND batch AS row
                                MERGE (:Key {{val: row.v}})
                            $$) as (v agtype);
                        """
                        cur.execute(q)

            # D. Load Events & Edges
            edge_batches = [edge_list[i:i+BATCH_SIZE] for i in range(0, len(edge_list), BATCH_SIZE)]
            
            with Timer(f"Loading {len(rows)} Events & Edges"):
                with conn.cursor() as cur:
                    cur.execute("LOAD 'age'; SET search_path = ag_catalog, '$user', public;")
                    for batch in edge_batches:
                        # FIXED: Batch is already a list of strings like "{id: '...', key: '...'}"
                        # Just join them with commas
                        batch_str = "[" + ",".join(batch) + "]"
                        
                        q = f"""
                            SELECT * FROM cypher('{GRAPH_NAME}', $$
                                WITH {batch_str} AS batch
                                UNWIND batch AS row
                                MERGE (e:Event {{id: row.id}})
                                WITH e, row
                                MATCH (k:Key {{val: row.key}})
                                MERGE (e)-[:HAS_KEY]->(k)
                            $$) as (v agtype);
                        """
                        cur.execute(q)
            
            # E. Mark Synced
            with conn.cursor() as cur:
                cur.execute("UPDATE public.events SET status = 'GRAPH_SYNCED' WHERE status = 'PENDING'")
            
            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Graph Projection Failed: {e}")
            raise
        finally:
            conn.close()

    # --------------------------------------------------------------------------
    # STEP 3: STITCHING
    # --------------------------------------------------------------------------
    def stitch_using_age(self):
        conn = self.get_conn()
        conn.autocommit = False
        
        try:
            # Fetch Batch
            with conn.cursor() as cur:
                cur.execute("SELECT event_id FROM public.events WHERE status = 'GRAPH_SYNCED' LIMIT 5000")
                target_ids = [str(r[0]) for r in cur.fetchall()]
            
            if not target_ids: return False

            processed_ids = set()
            new_journeys_io = StringIO()
            update_map_io = StringIO()
            merge_map_io = StringIO()
            journeys_to_delete = set()

            with Timer(f"Stitching Batch ({len(target_ids)} events)"):
                with conn.cursor() as cur:
                    cur.execute("LOAD 'age'; SET search_path = ag_catalog, '$user', public;")
                    
                    for eid in target_ids:
                        if eid in processed_ids: continue
                        
                        # AGE Traversal
                        cypher = f"""
                            SELECT * FROM cypher('{GRAPH_NAME}', $$
                                MATCH (start:Event {{id: '{eid}'}})-[:HAS_KEY*..4]-(connected:Event)
                                RETURN connected.id
                            $$) as (id agtype);
                        """
                        cur.execute(cypher)
                        
                        cluster = {eid}
                        for r in cur.fetchall():
                            cluster.add(r[0].replace('"', ''))
                        
                        processed_ids.update(cluster)
                        
                        # Check Relational DB
                        cluster_list = list(cluster)
                        cur.execute("SELECT DISTINCT journey_id FROM public.events WHERE event_id = ANY(%s::uuid[]) AND journey_id IS NOT NULL", (cluster_list,))
                        existing_jids = [str(r[0]) for r in cur.fetchall()]
                        
                        final_jid = None
                        
                        if not existing_jids:
                            final_jid = str(uuid.uuid4())
                            new_journeys_io.write(f"{final_jid}\n")
                        else:
                            final_jid = existing_jids[0]
                            if len(existing_jids) > 1:
                                for loser in existing_jids[1:]:
                                    journeys_to_delete.add(loser)
                                    merge_map_io.write(f"{final_jid}\t{loser}\n")
                        
                        for member_id in cluster_list:
                            update_map_io.write(f"{final_jid}\t{member_id}\n")

                new_journeys_io.seek(0)
                update_map_io.seek(0)
                merge_map_io.seek(0)

            # Bulk Writes
            with Timer("Applying SQL Updates"):
                with conn.cursor() as cur:
                    cur.copy_from(new_journeys_io, 'public.journeys', columns=('journey_id',))
                    
                    cur.execute("CREATE TEMP TABLE updates (jid UUID, eid UUID) ON COMMIT DROP")
                    cur.copy_from(update_map_io, 'updates', columns=('jid', 'eid'))
                    
                    cur.execute("""
                        UPDATE public.events e 
                        SET journey_id = u.jid, status = 'STITCHED'
                        FROM updates u
                        WHERE e.event_id = u.eid
                    """)
                    
                    if merge_map_io.getvalue():
                        cur.execute("CREATE TEMP TABLE merges (wid UUID, lid UUID) ON COMMIT DROP")
                        cur.copy_from(merge_map_io, 'merges', columns=('wid', 'lid'))
                        cur.execute("UPDATE public.events e SET journey_id = m.wid FROM merges m WHERE e.journey_id = m.lid")
                        cur.execute("DELETE FROM public.journeys WHERE journey_id = ANY(%s::uuid[])", (list(journeys_to_delete),))

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Stitching Failed: {e}")
            raise
        finally:
            conn.close()

    def verify_completeness(self, target):
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.journeys")
            cnt = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM public.events")
            ec = cur.fetchone()[0]
        print(f"\n📊 Final State: {ec} Events | {cnt} Journeys (Target: {target})")
        conn.close()

# --- GENERATOR ---
def generate_data(n_journeys):
    logger.info(f"🏭 Generating {n_journeys} clusters...")
    for i in range(n_journeys):
        e = f"u{i}@x.com"
        p = f"555-{i}"
        yield {"id": str(uuid.uuid4()), "app": "Web", "keys": [f"e:{e}"], "pl": {}}
        yield {"id": str(uuid.uuid4()), "app": "Mob", "keys": [f"p:{p}"], "pl": {}}
        yield {"id": str(uuid.uuid4()), "app": "Back", "keys": [f"e:{e}", f"p:{p}"], "pl": {}}

def main():
    pipe = AgeNativePipeline()
    pipe.init_db()
    
    TARGET = 1000
    data = list(generate_data(TARGET))
    
    # Ingest
    chunks = [data[i:i+5000] for i in range(0, len(data), 5000)]
    with concurrent.futures.ProcessPoolExecutor(max_workers=WORKERS) as exe:
        exe.map(pipe.ingest_worker, chunks)
    
    pipe.move_staging_to_main()
    
    # Graph Ops
    pipe.project_to_graph()
    
    logger.info("🔄 Starting Stitching Loop...")
    while True:
        has_more = pipe.stitch_using_age()
        if not has_more: break
    
    pipe.verify_completeness(TARGET)

if __name__ == "__main__":
    main()