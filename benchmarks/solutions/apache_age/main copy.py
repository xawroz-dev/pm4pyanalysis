import psycopg2
from psycopg2 import pool
import json
import time
import uuid
from datetime import datetime
from collections import defaultdict
import sys
import os
import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from io import StringIO

# Add common directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - AGE - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("AGE")


# ==================================================================
#                  HIGH-SPEED AGE MANAGER (MODE B)
# ==================================================================
class ApacheAgeJourneyManager(JourneyManager):
    """
    Ultra-optimized Apache AGE ingestion using COPY (Mode B).
    - COPY for Events, Correlations, HAS_KEY
    - SQL+sequence for PART_OF
    - Cypher used only for SELECT
    - Python counters for VID & HAS_KEY IDs
    """

    # ------------------------------------------------------------
    # INIT + CONNECTION HANDLING
    # ------------------------------------------------------------
    def __init__(self, db, pool_size=10):
        self.db = db
        self.graph = "benchmark_graph"

        # internal ID counters
        self.next_vid = 1
        self.next_has_key_edge_id = 1

        self._init_pool(pool_size)
        self.conn = self.pool.getconn()
        self.conn.autocommit = True

        self._wait_for_db()
        self._load_age(self.conn)

    def _init_pool(self, size):
        self.pool = pool.ThreadedConnectionPool(
            minconn=2, maxconn=size, **self.db
        )

    def _wait_for_db(self):
        while True:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                break
            except Exception:
                log.info("Waiting for DB...")
                time.sleep(1)

    def _load_age(self, conn):
        with conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute("SET search_path = ag_catalog, public;")

    def _new_conn(self):
        c = self.pool.getconn()
        c.autocommit = True
        self._load_age(c)
        return c

    # ------------------------------------------------------------
    # REQUIRED BY ABSTRACT BASE CLASS
    # ------------------------------------------------------------
    def setup(self):
        """Creates graph + tables + sequences."""
        self._create_structures()

    def clean(self):
        """Drops + recreates graph."""
        c = self._new_conn()
        try:
            with c.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM ag_graph WHERE name = %s;", (self.graph,))
                if cur.fetchone()[0] > 0:
                    cur.execute(f"SELECT drop_graph('{self.graph}', true);")

            self._create_structures()
        finally:
            self.pool.putconn(c)

    # ------------------------------------------------------------
    # GRAPH / SCHEMA CREATION
    # ------------------------------------------------------------
    def _create_structures(self):
        c = self._new_conn()
        try:
            with c.cursor() as cur:

                # ensure graph exists
                cur.execute("CREATE EXTENSION IF NOT EXISTS age;")
                cur.execute("LOAD 'age';")

                cur.execute("SELECT count(*) FROM ag_graph WHERE name = %s;", (self.graph,))
                exists = cur.fetchone()[0] > 0

                if not exists:
                    cur.execute(f"SELECT create_graph('{self.graph}');")

                # create vlabels
                for label in ["Event", "Correlation", "Journey"]:
                    cur.execute(f"SELECT create_vlabel('{self.graph}', '{label}');")

                # create elabels
                for label in ["HAS_KEY", "PART_OF"]:
                    cur.execute(f"SELECT create_elabel('{self.graph}', '{label}');")

                # create sequences safely
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_class WHERE relname = 'part_of_id_seq'
                        ) THEN
                            CREATE SEQUENCE {self.graph}.part_of_id_seq;
                        END IF;
                    END$$;
                """)

                # GIN index on properties
                for L in ["Event", "Correlation", "Journey"]:
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{L}_gin
                        ON "{self.graph}"."{L}"
                        USING GIN (properties);
                    """)

            log.info("Graph + indexes ready.")
        finally:
            self.pool.putconn(c)

    # ==================================================================
    #                           COPY INGESTION
    # ==================================================================
    def ingest_batch(self, batch: List[Dict[str, Any]]):
        if not batch:
            return

        # prepare COPY buffers
        ev_buf = StringIO()
        corr_buf = StringIO()
        haskey_buf = StringIO()

        existing_corr = set()

        now = datetime.utcnow().isoformat()

        for ev in batch:
            vid = self.next_vid
            self.next_vid += 1

            # Write Event row: vid \t properties_json
            ev_props = {
                "id": ev["id"],
                "status": "NEW",
                "created_at": now,
                "payload": ev["payload"]
            }
            ev_buf.write(f"{vid}\t{json.dumps(ev_props)}\n")

            # HAS_KEY edges
            for cid in ev["correlation_ids"]:
                if cid not in existing_corr:
                    # correlation node
                    c_vid = self.next_vid
                    self.next_vid += 1
                    existing_corr.add(cid)

                    corr_buf.write(f"{c_vid}\t{json.dumps({'id': cid})}\n")

                # edge id
                e_id = self.next_has_key_edge_id
                self.next_has_key_edge_id += 1

                haskey_buf.write(f"{e_id}\t{vid}\t{c_vid}\n")

        # perform COPY in DB
        conn = self._new_conn()
        try:
            with conn.cursor() as cur:
                # Event
                ev_buf.seek(0)
                cur.copy_expert(
                    f"""
                    COPY "{self.graph}"."Event"(vid, properties)
                    FROM STDIN WITH (FORMAT text)
                    """,
                    ev_buf
                )

                # Correlation
                corr_buf.seek(0)
                cur.copy_expert(
                    f"""
                    COPY "{self.graph}"."Correlation"(vid, properties)
                    FROM STDIN WITH (FORMAT text)
                    """,
                    corr_buf
                )

                # HAS_KEY edges
                haskey_buf.seek(0)
                cur.copy_expert(
                    f"""
                    COPY "{self.graph}"."HAS_KEY"(id, start_id, end_id)
                    FROM STDIN WITH (FORMAT text)
                    """,
                    haskey_buf
                )

        finally:
            self.pool.putconn(conn)

    # ==================================================================
    #                         EVENT PROCESSING
    # ==================================================================
    def process_events(self):
        BATCH = 5000

        while True:
            conn = self._new_conn()
            try:
                with conn.cursor() as cur:
                    # fetch NEW events
                    cur.execute(f"""
                        SELECT * FROM cypher('{self.graph}', $$
                            MATCH (e:Event {{status:'NEW'}})
                            WITH e LIMIT {BATCH}
                            MATCH (e)-[:HAS_KEY]->(c:Correlation)
                            RETURN e.id, collect(c.id)
                        $$) AS (eid agtype, cids agtype);
                    """)
                    rows = cur.fetchall()

                if not rows:
                    break

                events = []
                all_cids = set()
                for eid, cids in rows:
                    e = json.loads(eid)
                    c = json.loads(cids)
                    events.append({"id": e, "cids": c})
                    all_cids.update(c)

                # lookup journeys
                cid_list = list(all_cids)

                wrapper = {"batch": cid_list}
                j_lookup = json.dumps(wrapper)

                cid_to_j = defaultdict(list)
                j_created_at = {}

                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT * FROM cypher('{self.graph}', $$
                            WITH $batch AS items
                            MATCH (c:Correlation)-[:PART_OF]->(j:Journey)
                            WHERE c.id IN items
                            RETURN c.id, j.id, j.created_at
                        $$, %s::agtype)
                        AS (cid agtype, jid agtype, cat agtype);
                    """, (j_lookup,))

                    for cid, jid, cat in cur.fetchall():
                        cid = json.loads(cid)
                        jid = json.loads(jid)
                        cat = json.loads(cat)
                        cid_to_j[cid].append(jid)
                        j_created_at[jid] = cat

                actions = self._compute_actions(events, cid_to_j, j_created_at)

                # apply writes
                self._apply_actions(conn, actions)

            finally:
                self.pool.putconn(conn)

    # ------------------------------------------------------------
    # UNION FIND — GROUP CIDS + JOURNEYS
    # ------------------------------------------------------------
    def _compute_actions(self, events, cid_to_j, jmap):
        parent = {}

        def find(x):
            parent.setdefault(x, x)
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        # union within events
        for ev in events:
            cids = ev["cids"]
            if cids:
                base = cids[0]
                for cc in cids[1:]:
                    union(base, cc)

                for cid in cids:
                    for jid in cid_to_j.get(cid, []):
                        union(cid, jid)

        groups = defaultdict(lambda: {"cids": set(), "jids": set()})
        for ev in events:
            for cid in ev["cids"]:
                groups[find(cid)]["cids"].add(cid)
                for jid in cid_to_j.get(cid, []):
                    groups[find(cid)]["jids"].add(jid)

        # build actions
        actions = {
            "new_journeys": [],
            "cid_links": [],
            "merges": [],
            "events_done": [e["id"] for e in events]
        }

        now = datetime.utcnow().isoformat()

        for root, grp in groups.items():
            jids = sorted(list(grp["jids"]), key=lambda x: jmap.get(x, now))

            if not jids:
                jid = f"journey_{uuid.uuid4().hex}"
                actions["new_journeys"].append({"jid": jid, "created_at": now})
            else:
                jid = jids[0]
                for loser in jids[1:]:
                    actions["merges"].append({"winner": jid, "loser": loser})

            for cid in grp["cids"]:
                actions["cid_links"].append({"cid": cid, "jid": jid})

        return actions

    # ==================================================================
    #                     APPLY ALL ACTIONS (SQL)
    # ==================================================================
    def _apply_actions(self, conn, act):

        with conn.cursor() as cur:

            # new journeys
            for j in act["new_journeys"]:
                cur.execute(f"""
                    INSERT INTO "{self.graph}"."Journey"(vid, properties)
                    VALUES (DEFAULT, %s);
                """, (json.dumps({"id": j["jid"], "created_at": j["created_at"]}),))

            # cid_links => PART_OF edges
            for link in act["cid_links"]:
                cid = link["cid"]
                jid = link["jid"]
                cur.execute(f"""
                    SELECT cypher('{self.graph}', $$
                        MATCH (c:Correlation {{id:'{cid}'}}), 
                              (j:Journey {{id:'{jid}'}})
                        CREATE (c)-[:PART_OF]->(j)
                    $$);
                """)

            # merges
            for m in act["merges"]:
                w = m["winner"]
                l = m["loser"]

                # rewire
                cur.execute(f"""
                    SELECT cypher('{self.graph}', $$
                        MATCH (loser:Journey {{id:'{l}'}})
                        MATCH (winner:Journey {{id:'{w}'}})
                        MATCH (c:Correlation)-[r:PART_OF]->(loser)
                        DELETE r
                        CREATE (c)-[:PART_OF]->(winner)
                    $$);
                """)

                # delete loser
                cur.execute(f"""
                    SELECT cypher('{self.graph}', $$
                        MATCH (l:Journey {{id:'{l}'}})
                        DETACH DELETE l
                    $$);
                """)

            # mark events processed
            for eid in act["events_done"]:
                cur.execute(f"""
                    SELECT cypher('{self.graph}', $$
                        MATCH (e:Event {{id:'{eid}'}})
                        SET e.status='PROCESSED'
                    $$);
                """)

    # ==================================================================
    #                     GET JOURNEY FOR EVENT
    # ==================================================================
    def get_journey(self, event_id: str):
        conn = self._new_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM cypher('{self.graph}', $$
                        MATCH (e:Event {{id:'{event_id}'}})-[:HAS_KEY]->(:Correlation)-[:PART_OF]->(j)
                        MATCH (j)<-[:PART_OF]-(:Correlation)<-[:HAS_KEY]-(ev:Event)
                        RETURN j.id, collect(ev.id)
                    $$) AS (jid agtype, evs agtype);
                """)
                row = cur.fetchone()
                if not row:
                    return None

                jid = json.loads(row[0])
                evs = json.loads(row[1])
                return {"journey_id": jid, "events": evs}
        finally:
            self.pool.putconn(conn)

    def close(self):
        self.pool.closeall()


# =====================================================================
#                    RUN BENCHMARK
# =====================================================================
if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching

    DB = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": 5436,
    }

    jm = ApacheAgeJourneyManager(DB)
    jm.clean()

    log.info("Generating data...")
    data, ingest_time = generate_traffic(jm, 1000, 5, 30, 1000)

    log.info("Processing...")
    t0 = time.time()
    jm.process_events()
    process_time = time.time() - t0

    log.info(f"INGEST: {ingest_time:.2f}s | PROCESS: {process_time:.2f}s")

    validate_stitching(jm, data)
    jm.close()
