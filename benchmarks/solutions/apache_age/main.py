import json
import logging
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from common.interface import JourneyManager  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ApacheAGE")


class ApacheAgeJourneyManager(JourneyManager):
    """
    SQL-First Apache AGE Journey Manager (Target < 15s).
    
    Architecture:
      1. Direct Table Access: Bypasses Cypher parser entirely.
      2. Manual ID Generation: Simulates AGE's graph ID logic (Label ID << 48 | Sequence).
      3. Bulk COPY: Uses Postgres COPY protocol for max throughput.
      4. In-Memory WCC: Stitching logic runs in Python, reads/writes via SQL.
    """

    def __init__(self, db_config: Dict[str, Any], pool_size: int = 20):
        self.db_config = db_config
        self.graph_name = "benchmark_graph"
        self.pool_size = pool_size
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self._age_initialized_conn_ids = set()
        self.label_ids = {}

        logger.info("Initializing SQL-First AGE Manager...")
        self._init_connection_pool()

    def _init_connection_pool(self) -> None:
        try:
            self.connection_pool = pool.ThreadedConnectionPool(minconn=5, maxconn=self.pool_size, **self.db_config)
        except Exception as e:
            logger.error("Pool init failed: %s", e)
            raise

    def _get_connection(self):
        conn = self.connection_pool.getconn()
        conn.autocommit = True
        return conn

    def _return_connection(self, conn) -> None:
        self.connection_pool.putconn(conn)

    def _ensure_age_loaded(self, conn, cursor) -> None:
        if id(conn) not in self._age_initialized_conn_ids:
            cursor.execute("LOAD 'age';")
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
            self._age_initialized_conn_ids.add(id(conn))

    def _fetch_label_ids(self):
        """Cache internal AGE label IDs for manual ID generation."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                self._ensure_age_loaded(conn, cursor)
                # Get Graph ID
                cursor.execute("SELECT graphid FROM ag_graph WHERE name = %s", (self.graph_name,))
                row = cursor.fetchone()
                if not row:
                    logger.error(f"Graph {self.graph_name} not found in ag_graph!")
                    return
                graph_id = row[0]
                
                # Get Label IDs
                cursor.execute("SELECT name, id FROM ag_label WHERE graph = %s", (graph_id,))
                rows = cursor.fetchall()
                self.label_ids = {row[0]: row[1] for row in rows}
                logger.info(f"Loaded Label IDs: {self.label_ids}")
        finally:
            self._return_connection(conn)

    def _gen_id(self, label: str, count: int = 1) -> List[int]:
        """
        Generate valid AGE Graph IDs manually.
        Format: (LabelID << 48) | Sequence
        """
        label_id = self.label_ids[label]
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Use the sequence associated with the label
                # Sequence name format: "{graph_name}"."{Label}_id_seq"
                # Note: AGE might use a different internal sequence naming, but usually it creates a serial/sequence.
                # Actually, AGE uses `nextval('"{graph_name}"."{Label}_id_seq"')`
                
                seq_name = f'"{self.graph_name}"."{label}_id_seq"'
                cursor.execute(f"SELECT nextval('{seq_name}') FROM generate_series(1, {count})")
                seq_vals = [row[0] for row in cursor.fetchall()]
                
                return [(label_id << 48) | seq for seq in seq_vals]
        finally:
            self._return_connection(conn)

    # --------------------------------------------------------------------
    # Setup
    # --------------------------------------------------------------------
    def setup(self) -> None:
        logger.info("Setting up graph with SQL-optimized Schema...")
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
                self._ensure_age_loaded(conn, cursor)

                cursor.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (self.graph_name,))
                if cursor.fetchone()[0] == 0:
                    logger.info("Creating graph...")
                    cursor.execute(f"SELECT create_graph('{self.graph_name}');")

                for lbl in ["Event", "Correlation", "Journey"]:
                    logger.info(f"Creating vlabel {lbl}...")
                    cursor.execute(f"SELECT create_vlabel('{self.graph_name}', '{lbl}') WHERE _label_id('{self.graph_name}', '{lbl}') = 0;")
                for lbl in ["HAS_KEY", "PART_OF"]:
                    logger.info(f"Creating elabel {lbl}...")
                    cursor.execute(f"SELECT create_elabel('{self.graph_name}', '{lbl}') WHERE _label_id('{self.graph_name}', '{lbl}') = 0;")

                # Indexes
                logger.info("Creating indexes...")
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS "idx_event_status" 
                    ON "{self.graph_name}"."Event" (agtype_access_operator(VARIADIC ARRAY[properties, '"status"'::agtype]));
                """)
                cursor.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS "idx_event_id" 
                    ON "{self.graph_name}"."Event" (agtype_access_operator(VARIADIC ARRAY[properties, '"id"'::agtype]));
                """)
                cursor.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS "idx_correlation_id" 
                    ON "{self.graph_name}"."Correlation" (agtype_access_operator(VARIADIC ARRAY[properties, '"id"'::agtype]));
                """)
                cursor.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS "idx_journey_id" 
                    ON "{self.graph_name}"."Journey" (agtype_access_operator(VARIADIC ARRAY[properties, '"id"'::agtype]));
                """)
                
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise
        finally:
            self._return_connection(conn)
        
        self._fetch_label_ids()

    def clean(self) -> None:
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                self._ensure_age_loaded(conn, cursor)
                try: cursor.execute(f"SELECT drop_graph('{self.graph_name}', true);")
                except: pass
        finally:
            self._return_connection(conn)
        self.setup()

    # --------------------------------------------------------------------
    # Ingestion: Direct SQL COPY
    # --------------------------------------------------------------------
    def ingest_batch(self, events_batch: List[Dict[str, Any]]) -> None:
        if not events_batch: return

        start_time = time.time()
        conn = self._get_connection()

        try:
            # 1. Prepare Data
            formatted_events = []
            batch_cids = set()
            for ev in events_batch:
                unique_cids = list(set(ev["correlation_ids"]))
                batch_cids.update(unique_cids)
                formatted_events.append({
                    "id": ev["id"],
                    "status": "NEW",
                    "created_at": datetime.now().isoformat(),
                    "payload": ev.get("payload", {}),
                    "cids": unique_cids
                })

            # 2. Generate IDs for Events
            event_graph_ids = self._gen_id("Event", len(formatted_events))
            
            # 3. Prepare Event CSV for COPY
            # Table: "{graph_name}"."Event" (id agtype, properties agtype)
            # We must format as COPY expects.
            # id: internal graph id (bigint)
            # properties: agtype string. e.g., '{"id": "..."}'::agtype
            
            event_buffer = StringIO()
            for i, ev in enumerate(formatted_events):
                gid = event_graph_ids[i]
                props = json.dumps({
                    "id": ev["id"],
                    "status": "NEW",
                    "created_at": ev["created_at"],
                    "payload": ev["payload"]
                })
                # Escape for CSV/COPY if needed, but simple JSON usually fine if no newlines
                # COPY format: id \t properties
                event_buffer.write(f"{gid}\t{props}\n")
            
            event_buffer.seek(0)
            
            with conn.cursor() as cursor:
                cursor.copy_expert(f'COPY "{self.graph_name}"."Event" (id, properties) FROM STDIN', event_buffer)

            # 4. Handle Correlations (Read-Diff-Write)
            # We need to find which CIDs already exist to get their Graph IDs
            # And create new ones for missing
            
            cids_list = list(batch_cids)
            cid_map = {} # cid_str -> graph_id
            
            if cids_list:
                # SQL Query to find existing
                # We query the table directly using JSON operator ->>
                with conn.cursor() as cursor:
                    # Chunking for safety
                    CHUNK = 1000
                    for i in range(0, len(cids_list), CHUNK):
                        chunk = cids_list[i:i+CHUNK]
                        cursor.execute(f"""
                            SELECT id, properties::text::jsonb ->> 'id' 
                            FROM "{self.graph_name}"."Correlation"
                            WHERE properties::text::jsonb ->> 'id' = ANY(%s)
                        """, (chunk,))
                        for row in cursor.fetchall():
                            cid_map[row[1]] = row[0]

            new_cids = [c for c in cids_list if c not in cid_map]
            
            if new_cids:
                new_cid_ids = self._gen_id("Correlation", len(new_cids))
                cid_buffer = StringIO()
                for i, cid in enumerate(new_cids):
                    gid = new_cid_ids[i]
                    cid_map[cid] = gid
                    props = json.dumps({"id": cid})
                    cid_buffer.write(f"{gid}\t{props}\n")
                
                cid_buffer.seek(0)
                with conn.cursor() as cursor:
                    cursor.copy_expert(f'COPY "{self.graph_name}"."Correlation" (id, properties) FROM STDIN', cid_buffer)

            # 5. Create Edges (HAS_KEY)
            # Table: "{graph_name}"."HAS_KEY" (id, start_id, end_id, properties)
            links = []
            for i, ev in enumerate(formatted_events):
                eid = event_graph_ids[i]
                for cid in ev["cids"]:
                    if cid in cid_map:
                        links.append((eid, cid_map[cid]))
            
            if links:
                edge_ids = self._gen_id("HAS_KEY", len(links))
                edge_buffer = StringIO()
                for i, (start, end) in enumerate(links):
                    gid = edge_ids[i]
                    edge_buffer.write(f"{gid}\t{start}\t{end}\t{{}}\n") # Empty properties
                
                edge_buffer.seek(0)
                with conn.cursor() as cursor:
                    cursor.copy_expert(f'COPY "{self.graph_name}"."HAS_KEY" (id, start_id, end_id, properties) FROM STDIN', edge_buffer)

            elapsed = time.time() - start_time
            logger.info(f"Ingested {len(events_batch)} events in {elapsed:.2f}s")

        finally:
            self._return_connection(conn)

    # --------------------------------------------------------------------
    # Processing: SQL-Based Fetch + In-Memory WCC + SQL Write
    # --------------------------------------------------------------------
    def process_events(self) -> None:
        logger.info("Processing Stitching Logic (SQL-First)...")
        BATCH_SIZE = 5000
        conn = self._get_connection()
        
        try:
            while True:
                # 1. Fetch NEW Events and their Correlations via SQL Joins
                # We join Event -> HAS_KEY -> Correlation
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT e.properties::text::jsonb ->> 'id' as eid, 
                               c.properties::text::jsonb ->> 'id' as cid
                        FROM "{self.graph_name}"."Event" e
                        JOIN "{self.graph_name}"."HAS_KEY" hk ON e.id = hk.start_id
                        JOIN "{self.graph_name}"."Correlation" c ON hk.end_id = c.id
                        WHERE e.properties::text::jsonb ->> 'status' = 'NEW'
                        LIMIT {BATCH_SIZE} * 5  -- Fetch more rows as 1 event has multiple CIDs
                    """)
                    rows = cursor.fetchall()
                
                if not rows: break
                
                # Build Event -> CIDs map
                event_cids = defaultdict(list)
                all_cids = set()
                for eid, cid in rows:
                    event_cids[eid].append(cid)
                    all_cids.add(cid)
                
                if not event_cids: break

                # 2. Find Existing Journeys for these CIDs
                # Join Correlation -> PART_OF -> Journey
                cid_to_j = defaultdict(list)
                existing_j = {}
                
                if all_cids:
                    with conn.cursor() as cursor:
                        # Chunking
                        cids_list = list(all_cids)
                        CHUNK = 1000
                        for i in range(0, len(cids_list), CHUNK):
                            chunk = cids_list[i:i+CHUNK]
                            cursor.execute(f"""
                                SELECT c.properties::text::jsonb ->> 'id', 
                                       j.properties::text::jsonb ->> 'id',
                                       j.properties::text::jsonb ->> 'created_at'
                                FROM "{self.graph_name}"."Correlation" c
                                JOIN "{self.graph_name}"."PART_OF" po ON c.id = po.start_id
                                JOIN "{self.graph_name}"."Journey" j ON po.end_id = j.id
                                WHERE c.properties::text::jsonb ->> 'id' = ANY(%s)
                            """, (chunk,))
                            
                            for cid, jid, ca in cursor.fetchall():
                                cid_to_j[cid].append(jid)
                                existing_j[jid] = ca

                # 3. In-Memory WCC
                parent = {}
                def find(x):
                    parent.setdefault(x, x)
                    if parent[x] != x: parent[x] = find(parent[x])
                    return parent[x]
                def union(a, b):
                    ra, rb = find(a), find(b)
                    if ra != rb: parent[ra] = rb

                # Union Events and CIDs
                for eid, cids in event_cids.items():
                    if not cids: continue
                    first = cids[0]
                    for other in cids[1:]: union(first, other)
                    for cid in cids:
                        if cid in cid_to_j:
                            for jid in cid_to_j[cid]: union(cid, jid)
                
                # Group
                groups = defaultdict(lambda: {"cids": set(), "jids": set()})
                for cid in all_cids:
                    groups[find(cid)]["cids"].add(cid)
                for jid in existing_j:
                    if jid in parent:
                        groups[find(jid)]["jids"].add(jid)

                new_journeys = []
                merges = [] # (winner_jid, loser_jid)
                cid_links = [] # (jid, cid)
                
                for root, group in groups.items():
                    jids = list(group["jids"])
                    target_jid = None
                    
                    if not jids:
                        target_jid = f"journey_{uuid.uuid4()}"
                        new_journeys.append({
                            "id": target_jid, 
                            "created_at": datetime.now().isoformat()
                        })
                    elif len(jids) == 1:
                        target_jid = jids[0]
                    else:
                        jids.sort(key=lambda x: existing_j[x])
                        target_jid = jids[0]
                        for loser in jids[1:]:
                            merges.append((target_jid, loser))
                    
                    for cid in group["cids"]:
                        cid_links.append((target_jid, cid))

                # 4. Bulk Writes (SQL)
                
                # Create New Journeys
                if new_journeys:
                    j_ids = self._gen_id("Journey", len(new_journeys))
                    j_buffer = StringIO()
                    for i, j in enumerate(new_journeys):
                        gid = j_ids[i]
                        props = json.dumps(j)
                        j_buffer.write(f"{gid}\t{props}\n")
                    j_buffer.seek(0)
                    with conn.cursor() as cursor:
                        cursor.copy_expert(f'COPY "{self.graph_name}"."Journey" (id, properties) FROM STDIN', j_buffer)

                # Handle Merges
                # For merges, we need to:
                # 1. Update PART_OF edges pointing to loser -> point to winner
                # 2. Delete loser Journey nodes
                jid_to_gid = {}
                if merges:
                    # We can do this via SQL UPDATE/DELETE
                    # Need to map JIDs to Graph IDs? 
                    # Or just use properties->>'id' in WHERE clause (slower but easier)
                    # Or fetch Graph IDs first.
                    # Let's use properties for simplicity, indexed.
                    
                    with conn.cursor() as cursor:
                        # 1. Re-link edges
                        # UPDATE "PART_OF" SET end_id = (SELECT id FROM Journey WHERE props->>id = winner)
                        # WHERE end_id = (SELECT id FROM Journey WHERE props->>id = loser)
                        # This is complex in bulk.
                        # Better: Fetch IDs of winners and losers
                        
                        all_merge_jids = set()
                        for w, l in merges:
                            all_merge_jids.add(w)
                            all_merge_jids.add(l)
                        
                        jid_to_gid = {}
                        cursor.execute(f"""
                            SELECT properties::text::jsonb ->> 'id', id 
                            FROM "{self.graph_name}"."Journey"
                            WHERE properties::text::jsonb ->> 'id' = ANY(%s)
                        """, (list(all_merge_jids),))
                        for r in cursor.fetchall():
                            jid_to_gid[r[0]] = r[1]
                        
                        # Prepare updates
                        # We need to update PART_OF edges.
                        # UPDATE "PART_OF" SET end_id = winner_gid WHERE end_id = loser_gid
                        
                        # We can execute many updates or a CASE statement.
                        # execute_values is good for inserts, for updates maybe a temp table?
                        # Let's try simple loop for merges (usually few) or batched UPDATE
                        
                        for w, l in merges:
                            if w in jid_to_gid and l in jid_to_gid:
                                w_gid = jid_to_gid[w]
                                l_gid = jid_to_gid[l]
                                cursor.execute(f"""
                                    UPDATE "{self.graph_name}"."PART_OF"
                                    SET end_id = %s::graphid
                                    WHERE end_id = %s::graphid
                                """, (w_gid, l_gid))
                                
                                cursor.execute(f"""
                                    DELETE FROM "{self.graph_name}"."Journey"
                                    WHERE id = %s::graphid
                                """, (l_gid,))

                # Link CIDs to Journeys
                if cid_links:
                    # We need Graph IDs for CIDs and JIDs
                    # Some JIDs are new (we have their GIDs in j_ids/new_journeys order)
                    # Some are existing (need fetch)
                    # CIDs: need fetch
                    
                    # Optimization: Maintain a cache of JID->GID and CID->GID in memory for this batch?
                    # We have jid_to_gid from merges, and we have new journey GIDs.
                    # We need to fetch GIDs for all target JIDs and CIDs in links.
                    
                    needed_jids = set(l[0] for l in cid_links)
                    needed_cids = set(l[1] for l in cid_links)
                    
                    # Update cache
                    # New journeys
                    for i, j in enumerate(new_journeys):
                        jid_to_gid[j["id"]] = j_ids[i]
                        
                    # Fetch missing
                    missing_jids = [j for j in needed_jids if j not in jid_to_gid]
                    if missing_jids:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                SELECT properties::text::jsonb ->> 'id', id 
                                FROM "{self.graph_name}"."Journey"
                                WHERE properties::text::jsonb ->> 'id' = ANY(%s)
                            """, (missing_jids,))
                            for r in cursor.fetchall():
                                jid_to_gid[r[0]] = r[1]
                                
                    cid_to_gid = {}
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            SELECT properties::text::jsonb ->> 'id', id 
                            FROM "{self.graph_name}"."Correlation"
                            WHERE properties::text::jsonb ->> 'id' = ANY(%s)
                        """, (list(needed_cids),))
                        for r in cursor.fetchall():
                            cid_to_gid[r[0]] = r[1]
                            
                    # Prepare Edges
                    # We need to avoid duplicates. "PART_OF" doesn't have unique constraint by default on (start, end)?
                    # AGE edges usually don't enforce uniqueness unless we added it.
                    # We should check existence or use INSERT ON CONFLICT if we added constraint.
                    # Or just INSERT and assume WCC logic prevents dupes (it should, as we group by component).
                    # But if we re-process, we might dupe.
                    # Let's assume we need to check.
                    # Or better: DELETE existing PART_OF for these CIDs and INSERT new (Re-link).
                    # Since a CID belongs to only 1 Journey.
                    
                    # Delete existing PART_OF for involved CIDs
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            DELETE FROM "{self.graph_name}"."PART_OF"
                            WHERE start_id = ANY(%s::graphid[])
                        """, (list(cid_to_gid.values()),))
                    
                    # Insert new edges
                    new_edges = []
                    for jid, cid in cid_links:
                        if jid in jid_to_gid and cid in cid_to_gid:
                            new_edges.append((cid_to_gid[cid], jid_to_gid[jid]))
                            
                    if new_edges:
                        edge_ids = self._gen_id("PART_OF", len(new_edges))
                        edge_buffer = StringIO()
                        for i, (start, end) in enumerate(new_edges):
                            gid = edge_ids[i]
                            edge_buffer.write(f"{gid}\t{start}\t{end}\t{{}}\n")
                        edge_buffer.seek(0)
                        with conn.cursor() as cursor:
                            cursor.copy_expert(f'COPY "{self.graph_name}"."PART_OF" (id, start_id, end_id, properties) FROM STDIN', edge_buffer)

                # 5. Mark Processed
                # UPDATE "Event" SET properties = properties || '{"status": "PROCESSED"}'
                # WHERE properties->>'id' IN (...)
                # This is slow if we update JSONB.
                # But we must.
                processed_eids = list(event_cids.keys())
                if processed_eids:
                    with conn.cursor() as cursor:
                        # Batch update?
                        # Or just set status.
                        # We can use a temp table to join update.
                        cursor.execute(f"""
                            UPDATE "{self.graph_name}"."Event"
                            SET properties = jsonb_set(properties::text::jsonb, '{{status}}', '"PROCESSED"')::text::agtype
                            WHERE properties::text::jsonb ->> 'id' = ANY(%s)
                        """, (processed_eids,))

                logger.info(f"Stitched batch of {len(processed_eids)}")

        finally:
            self._return_connection(conn)

    def get_journey(self, event_id: str) -> Optional[Dict[str, Any]]:
        # Use SQL for retrieval too
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Find Journey ID for Event
                cursor.execute(f"""
                    SELECT j.properties::text::jsonb ->> 'id'
                    FROM "{self.graph_name}"."Event" e
                    JOIN "{self.graph_name}"."HAS_KEY" hk ON e.id = hk.start_id
                    JOIN "{self.graph_name}"."Correlation" c ON hk.end_id = c.id
                    JOIN "{self.graph_name}"."PART_OF" po ON c.id = po.start_id
                    JOIN "{self.graph_name}"."Journey" j ON po.end_id = j.id
                    WHERE e.properties::text::jsonb ->> 'id' = %s
                    LIMIT 1
                """, (event_id,))
                row = cursor.fetchone()
                if not row: return None
                jid = row[0]
                
                # Find all events for Journey
                cursor.execute(f"""
                    SELECT e.properties::text::jsonb ->> 'id'
                    FROM "{self.graph_name}"."Journey" j
                    JOIN "{self.graph_name}"."PART_OF" po ON j.id = po.end_id
                    JOIN "{self.graph_name}"."Correlation" c ON po.start_id = c.id
                    JOIN "{self.graph_name}"."HAS_KEY" hk ON c.id = hk.end_id
                    JOIN "{self.graph_name}"."Event" e ON hk.start_id = e.id
                    WHERE j.properties::text::jsonb ->> 'id' = %s
                """, (jid,))
                events = [r[0] for r in cursor.fetchall()]
                
                return {"journey_id": jid, "events": events}
        finally:
            self._return_connection(conn)

    def close(self):
        if self.connection_pool: self.connection_pool.closeall()

if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching

    DB_CONFIG = {"dbname": "postgres", "user": "postgres", "password": "password", "host": "localhost", "port": 5436}
    jm = ApacheAgeJourneyManager(DB_CONFIG)
    
    # Wait for DB
    for _ in range(10):
        try: 
            jm.clean()
            break
        except: time.sleep(1)

    # Generate Data
    generated_data, ingest_time = generate_traffic(jm, 1000, 50, 5)
    
    # Process
    start = time.time()
    jm.process_events()
    proc_time = time.time() - start

    logger.info(f"Ingest: {ingest_time:.2f}s | Process: {proc_time:.2f}s")
    
    # Validation
    validate_stitching(jm, generated_data)
    jm.close()
