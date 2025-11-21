import time
import random
import uuid
from journey_manager import JourneyManager

# Database Configuration
# Ensure this matches docker-compose.yml
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5435
}

NUM_JOURNEYS = 1000
EVENTS_PER_JOURNEY = 5

def generate_traffic(jm):
    print(f"\n[Generator] Generating {NUM_JOURNEYS} journeys with {EVENTS_PER_JOURNEY} events each...")
    
    generated_data = {} # journey_index -> list of event_ids
    batch_events = []
    BATCH_SIZE = 1000

    start_time = time.time()
    
    for j_idx in range(NUM_JOURNEYS):
        # Create a unique base key for this journey to avoid collisions with other journeys
        base_key = str(uuid.uuid4())[:8]
        
        event_ids = []
        
        # We want to chain them:
        # E1: [K1]
        # E2: [K1, K2]
        # E3: [K2, K3]
        # ...
        
        # Generate keys first
        keys = [f"key_{base_key}_{i}" for i in range(EVENTS_PER_JOURNEY)]
        
        for i in range(EVENTS_PER_JOURNEY):
            e_id = f"event_{base_key}_{i}"
            event_ids.append(e_id)
            
            # Determine correlation IDs for this event
            c_ids = []
            
            # Current key
            c_ids.append(keys[i])
            
            # Previous key (to link to previous event)
            if i > 0:
                c_ids.append(keys[i-1])
            
            batch_events.append({
                "id": e_id,
                "correlation_ids": c_ids,
                "payload": {}
            })
            
        generated_data[j_idx] = event_ids
        
        if len(batch_events) >= BATCH_SIZE:
            jm.ingest_events_batch(batch_events)
            batch_events = []
            print(f"  -> Ingested batch... (Total journeys: {j_idx + 1})")

    # Ingest remaining
    if batch_events:
        jm.ingest_events_batch(batch_events)

    duration = time.time() - start_time
    print(f"[Generator] Ingestion complete in {duration:.2f} seconds.")
    return generated_data

def validate_stitching(jm, generated_data):
    print("\n[Validation] Validating random sample of journeys...")
    
    sample_size = 20
    samples = random.sample(list(generated_data.keys()), min(sample_size, len(generated_data)))
    
    success_count = 0
    
    for j_idx in samples:
        expected_events = set(generated_data[j_idx])
        
        # Pick any event from this journey to query
        probe_event = list(expected_events)[0]
        
        journey_info = jm.get_journey(probe_event)
        
        if not journey_info:
            print(f"  [FAIL] Journey not found for event {probe_event}")
            continue
            
        actual_events = set(journey_info['events'])
        
        # Check if all expected events are present
        if expected_events.issubset(actual_events):
            # Check if there are extra events (unexpected merging)
            if len(actual_events) == len(expected_events):
                # Perfect match
                success_count += 1
            else:
                print(f"  [WARN] Journey {journey_info['journey_id']} has {len(actual_events)} events, expected {len(expected_events)}. (Possible over-merging)")
        else:
            missing = expected_events - actual_events
            print(f"  [FAIL] Journey {journey_info['journey_id']} missing events: {missing}")

    print(f"\n[Validation] Result: {success_count}/{len(samples)} passed.")
    if success_count == len(samples):
        print("SUCCESS: All sampled journeys are correctly stitched.")
    else:
        print("FAILURE: Some journeys failed validation.")

def main():
    print("=== Apache AGE Scale Test ===")
    
    # Initialize Manager
    jm = JourneyManager(DB_CONFIG)
    
    # Wait for DB
    connected = False
    for i in range(10):
        try:
            jm.connect()
            connected = True
            break
        except:
            print("Waiting for Database...")
            time.sleep(2)
    
    if not connected:
        print("Could not connect to DB.")
        return

    # Setup
    print("\n[Step 1] Setting up Graph...")
    jm.setup_graph()
    print("Cleaning old data...")
    jm.clean_data()
    
    # Generate & Ingest
    generated_data = generate_traffic(jm)
    
    # Process
    print("\n[Step 2] Processing Events...")
    process_start = time.time()
    jm.process_new_events()
    process_duration = time.time() - process_start
    print(f"Processing complete in {process_duration:.2f} seconds.")
    
    # Validate
    validate_stitching(jm, generated_data)

if __name__ == "__main__":
    main()
