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

graph_name = "test_graph"

with conn.cursor() as cursor:
    # Setup
    cursor.execute("LOAD 'age';")
    cursor.execute("SET search_path = ag_catalog, '$user', public;")
    
    cursor.execute(f"SELECT count(*) FROM ag_graph WHERE name='{graph_name}'")
    if cursor.fetchone()[0] == 0:
        cursor.execute(f"SELECT create_graph('{graph_name}');")

    # Test 1: Simple query with string literal params
    print("Test 1: String literal")
    try:
        sql = f"SELECT * FROM cypher('{graph_name}', $$ RETURN $x $$, '{{\"x\": 1}}') as (v agtype);"
        cursor.execute(sql)
        print(cursor.fetchall())
    except Exception as e:
        print(f"Test 1 failed: {e}")

    # Test 2: Psycopg2 binding
    print("\nTest 2: Psycopg2 binding")
    try:
        sql = f"SELECT * FROM cypher('{graph_name}', $$ RETURN $x $$, %s) as (v agtype);"
        cursor.execute(sql, ('{"x": 2}',))
        print(cursor.fetchall())
    except Exception as e:
        print(f"Test 2 failed: {e}")

    # Test 3: Psycopg2 binding with agtype cast
    print("\nTest 3: Psycopg2 binding with cast")
    try:
        sql = f"SELECT * FROM cypher('{graph_name}', $$ RETURN $x $$, %s::agtype) as (v agtype);"
        cursor.execute(sql, ('{"x": 3}',))
        print(cursor.fetchall())
    except Exception as e:
        print(f"Test 3 failed: {e}")

    # Cleanup
    cursor.execute(f"SELECT drop_graph('{graph_name}', true);")
