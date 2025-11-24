import psycopg2
import json

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5436
}

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True

def run_query(query):
    with conn.cursor() as cursor:
        cursor.execute("LOAD 'age';")
        cursor.execute("SET search_path = ag_catalog, '$user', public;")
        full_query = f"SELECT * FROM cypher('benchmark_graph', $$ {query} $$) as (v agtype);"
        try:
            cursor.execute(full_query)
            print("Success!")
            return cursor.fetchall()
        except Exception as e:
            print(f"Error: {e}")

# Test 1: JSON style (quoted keys)
print("Test 1: JSON style")
data = [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}]
json_str = json.dumps(data).replace("'", "''")
q1 = f"UNWIND {json_str} as row RETURN row"
run_query(q1)

# Test 2: Cypher style (unquoted keys)
print("\nTest 2: Cypher style")
cypher_str = "[{id: '1', val: 'a'}, {id: '2', val: 'b'}]"
q2 = f"UNWIND {cypher_str} as row RETURN row"
run_query(q2)
