from solutions.apache_age.main import ApacheAgeJourneyManager
from common.generator import generate_traffic
from common.validator import validate_stitching
import time

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5436
}

def run_test():
    print("Initializing Manager...")
    jm = ApacheAgeJourneyManager(DB_CONFIG)
    
    print("Connecting...")
    try:
        jm.connect()
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print("Cleaning...")
    jm.clean()
    
    print("Generating Traffic (10 journeys)...")
    generated_data, ingest_time = generate_traffic(jm, 10, 5, batch_size=10)
    
    print(f"Ingestion done in {ingest_time:.2f}s")
    
    print("Processing Events...")
    start = time.time()
    jm.process_events()
    duration = time.time() - start
    print(f"Processing done in {duration:.2f}s")
    
    print("Validating...")
    validate_stitching(jm, generated_data)

if __name__ == "__main__":
    run_test()
