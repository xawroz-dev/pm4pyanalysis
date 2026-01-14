import time
import subprocess
import os
import sys
import nest_asyncio
from gremlin_python.driver.client import Client

# Apply nest_asyncio to fix event loop issues
nest_asyncio.apply()

# Configuration
GREMLIN_URI = os.environ.get('GREMLIN_URI', 'ws://localhost:8182/gremlin')

def wait_for_connection(retries=20, delay=5):
    client = Client(GREMLIN_URI, 'g')
    try:
        for i in range(retries):
            try:
                client.submit("g.V().limit(1)").all().result()
                print("Connected to JanusGraph.")
                return True
            except Exception as e:
                print(f"Waiting for JanusGraph... ({i+1}/{retries}) - {e}")
                time.sleep(delay)
        return False
    finally:
        client.close()

def run_benchmark_for_scale(journeys, results_file):
    usecase = f"benchmark_{journeys}"
    print(f"\n--- Starting Benchmark for {journeys} Journeys (Usecase: {usecase}) ---")
    
    client = Client(GREMLIN_URI, 'g')
    
    try:
        # 1. Cleanup
        try:
            print("Cleaning old data...")
            client.submit(f"g.V().has('usecase', '{usecase}').drop()").all().result()
        except Exception as e:
            print(f"Cleanup error: {e}")

        # 2. Define Schema (Idempotent)
        print("Ensuring Schema...")
        with open('schema.groovy', 'r') as f:
            schema_script = f.read()
            full_script = schema_script + "\ndefineSchema(graph)"
            try:
                client.submit(full_script).all().result()
            except Exception as e:
                print(f"Schema error: {e}")

        # 3. Ingest
        print("Running Ingestion...")
        start_ingest = time.time()
        # Scale batch size with journeys
        batch_size = "500"
        if journeys >= 10000:
            batch_size = "2000"
            
        subprocess.run([
            sys.executable, "ingestor.py", 
            "--usecase", usecase,
            "--journeys", str(journeys),
            "--apps", "3",
            "--events_per_app", "30",
            "--batch_size", batch_size
        ], check=True)
        end_ingest = time.time()
        ingest_time = end_ingest - start_ingest
        print(f"Ingestion Time: {ingest_time:.2f}s")

        # 4. Stitch
        print("Running Stitching...")
        start_stitch = time.time()
        with open('stitching.groovy', 'r') as f:
            stitch_script = f.read()
            full_stitch_script = stitch_script + f"\nstitch('{usecase}')"
            result = client.submit(full_stitch_script).all().result()
            print(f"Stitching Result: {result}")
        end_stitch = time.time()
        stitch_time = end_stitch - start_stitch
        print(f"Stitching Time: {stitch_time:.2f}s")
        
        # 5. Validate
        print("Running Validation...")
        subprocess.run([sys.executable, "validator.py", "--usecase", usecase], check=True)
        
        total_time = ingest_time + stitch_time
        print(f"Total Execution Time: {total_time:.2f}s")
        
        # Save results
        with open(results_file, "a") as f:
            f.write(f"Journeys: {journeys}, Ingest: {ingest_time:.2f}s, Stitch: {stitch_time:.2f}s, Total: {total_time:.2f}s\n")
            
    except Exception as e:
        print(f"Benchmark failed for {journeys}: {e}")
        with open(results_file, "a") as f:
            f.write(f"Journeys: {journeys}, FAILED: {e}\n")
    finally:
        client.close()

def run_all_benchmarks():
    results_file = "benchmark_results.txt"
    # Clear previous results
    with open(results_file, "w") as f:
        f.write("Benchmark Results\n=================\n")
        
    if not wait_for_connection():
        print("Could not connect to JanusGraph. Exiting.")
        return

    scales = [100, 1000, 10000]
    
    for scale in scales:
        run_benchmark_for_scale(scale, results_file)

if __name__ == "__main__":
    run_all_benchmarks()
