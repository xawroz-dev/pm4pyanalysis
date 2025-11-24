from solutions.memgraph.main import MemgraphJourneyManager
from common.generator import generate_traffic
from common.validator import validate_stitching
import time

def run_test():
    print("Initializing Manager...")
    jm = MemgraphJourneyManager("bolt://localhost:7688")
    
    print("Setup...")
    try:
        jm.setup()
    except Exception as e:
        print(f"Setup failed: {e}")
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
    jm.close()

if __name__ == "__main__":
    run_test()
