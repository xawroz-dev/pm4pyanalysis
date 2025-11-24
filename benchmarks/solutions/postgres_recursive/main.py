import psycopg2
import json
import time
import uuid
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.interface import JourneyManager

class PostgresRecursiveJourneyManager(JourneyManager):
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        
    def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
    
    def setup(self):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    status TEXT,
                    created_at TIMESTAMP,
                    payload JSONB
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS correlation_keys (
                    key TEXT,
                    event_id TEXT REFERENCES events(id),
                    PRIMARY KEY (key, event_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS journeys (
                    id TEXT PRIMARY KEY,
                    created_at TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS event_journey (
                    event_id TEXT REFERENCES events(id),
                    journey_id TEXT REFERENCES journeys(id),
                    PRIMARY KEY (event_id, journey_id)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_corr_key ON correlation_keys(key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_corr_event ON correlation_keys(event_id)")
    
    def clean(self):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS event_journey CASCADE")
            cursor.execute("DROP TABLE IF EXISTS correlation_keys CASCADE")
            cursor.execute("DROP TABLE IF EXISTS events CASCADE")
            cursor.execute("DROP TABLE IF EXISTS journeys CASCADE")
        self.setup()
    
    def ingest_batch(self, events_batch):
        if not events_batch:
            return
        
        self.connect()
        with self.conn.cursor() as cursor:
            for ev in events_batch:
                cursor.execute("""
                    INSERT INTO events (id, status, created_at, payload)
                    VALUES (%s, %s, %s, %s)
                """, (
                    ev['id'],
                    'NEW',
                    datetime.now(),
                    json.dumps(ev.get('payload', {}))
                ))
                
                # Insert correlation keys
                for cid in ev['correlation_ids']:
                    cursor.execute("""
                        INSERT INTO correlation_keys (key, event_id)
                        VALUES (%s, %s)
                    """, (cid, ev['id']))
    
    def process_events(self):
        self.connect()
        
        # We need to process NEW events.
        # We can use a recursive CTE to find connected components.
        # But doing this for ALL new events at once might be heavy.
        # Let's pick one NEW event, find its component, process, repeat.
        
        while True:
            with self.conn.cursor() as cursor:
                # Pick a start node
                cursor.execute("SELECT id FROM events WHERE status = 'NEW' LIMIT 1")
                row = cursor.fetchone()
                if not row:
                    break
                
                start_id = row[0]
                
                # Recursive CTE to find all connected events
                # (event) -> (key) -> (event)
                cursor.execute("""
                    WITH RECURSIVE component AS (
                        -- Base case: start event
                        SELECT id FROM events WHERE id = %s
                        
                        UNION
                        
                        -- Recursive step: 
                        -- 1. From event to keys
                        -- 2. From keys to other events
                        SELECT ck2.event_id
                        FROM component c
                        JOIN correlation_keys ck1 ON c.id = ck1.event_id
                        JOIN correlation_keys ck2 ON ck1.key = ck2.key
                        WHERE ck2.event_id != c.id
                    )
                    SELECT DISTINCT id FROM component
                """, (start_id,))
                
                event_ids = [r[0] for r in cursor.fetchall()]
                
                # Find existing journeys for these events
                cursor.execute("""
                    SELECT DISTINCT j.id, j.created_at
                    FROM event_journey ej
                    JOIN journeys j ON ej.journey_id = j.id
                    WHERE ej.event_id = ANY(%s)
                    ORDER BY j.created_at
                """, (event_ids,))
                
                existing_journeys = [{"id": row[0], "created_at": row[1]} for row in cursor.fetchall()]
                
                if not existing_journeys:
                    journey_id = f"journey_{uuid.uuid4()}"
                    cursor.execute("""
                        INSERT INTO journeys (id, created_at)
                        VALUES (%s, %s)
                    """, (journey_id, datetime.now()))
                    
                elif len(existing_journeys) == 1:
                    journey_id = existing_journeys[0]['id']
                    
                else:
                    journey_id = existing_journeys[0]['id']
                    loser_ids = [j['id'] for j in existing_journeys[1:]]
                    
                    # Move events
                    cursor.execute("""
                        UPDATE event_journey
                        SET journey_id = %s
                        WHERE journey_id = ANY(%s)
                    """, (journey_id, loser_ids))
                    
                    # Delete losers
                    cursor.execute("""
                        DELETE FROM journeys
                        WHERE id = ANY(%s)
                    """, (loser_ids,))
                
                # Link events
                for eid in event_ids:
                    cursor.execute("""
                        INSERT INTO event_journey (event_id, journey_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (eid, journey_id))
                    
                    cursor.execute("""
                        UPDATE events
                        SET status = 'PROCESSED'
                        WHERE id = %s
                    """, (eid,))
    
    def get_journey(self, event_id):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT j.id, array_agg(e.id)
                FROM event_journey ej1
                JOIN journeys j ON ej1.journey_id = j.id
                JOIN event_journey ej2 ON j.id = ej2.journey_id
                JOIN events e ON ej2.event_id = e.id
                WHERE ej1.event_id = %s
                GROUP BY j.id
            """, (event_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    "journey_id": row[0],
                    "events": row[1]
                }
            return None

if __name__ == "__main__":
    from common.generator import generate_traffic
    from common.validator import validate_stitching
    
    DB_CONFIG = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": 5437
    }
    
    jm = PostgresRecursiveJourneyManager(DB_CONFIG)
    
    for i in range(30):
        try:
            jm.connect()
            break
        except:
            time.sleep(2)
    
    jm.setup()
    jm.clean()
    
    NUM_JOURNEYS = 100
    EVENTS_PER_APP = 5
    NUM_APPS = 4
    
    print("Starting Benchmark: PostgreSQL Recursive (Refactored)")
    generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)
    
    start_process = time.time()
    jm.process_events()
    process_time = time.time() - start_process
    
    print(f"Ingestion Time: {ingest_time:.2f}s")
    print(f"Processing Time: {process_time:.2f}s")
    print(f"Total Time: {ingest_time + process_time:.2f}s")
    
    validate_stitching(jm, generated_data)
