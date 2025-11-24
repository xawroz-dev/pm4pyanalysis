import psycopg2
import sys

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5436
}

def check_age():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, '$user', public;")
        
        # Check graph existence
        cur.execute("SELECT count(*) FROM ag_graph WHERE name = 'benchmark_graph'")
        exists = cur.fetchone()[0]
        print(f"Graph exists: {exists}")
        
        if exists:
            # Check node counts
            cur.execute("SELECT * FROM cypher('benchmark_graph', $$ MATCH (n) RETURN count(n) $$) as (count agtype);")
            count = cur.fetchone()[0]
            print(f"Total nodes: {count}")
            
            cur.execute("SELECT * FROM cypher('benchmark_graph', $$ MATCH (e:Event) RETURN count(e) $$) as (count agtype);")
            events = cur.fetchone()[0]
            print(f"Events: {events}")
            
            cur.execute("SELECT * FROM cypher('benchmark_graph', $$ MATCH (j:Journey) RETURN count(j) $$) as (count agtype);")
            journeys = cur.fetchone()[0]
            print(f"Journeys: {journeys}")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_age()
