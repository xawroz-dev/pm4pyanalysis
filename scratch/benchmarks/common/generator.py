import time
import uuid
import random

def generate_traffic(jm, num_journeys, events_per_app=5, num_apps=4):
    """
    Generates traffic for benchmarking.
    Structure:
    - num_journeys independent journeys
    - Each journey has num_apps applications participating
    - Each app generates events_per_app events
    - Apps are linked by shared correlation keys
    """
    print(f"[Generator] Generating {num_journeys} journeys with {num_apps} apps, {events_per_app} events each...")
    
    all_events = []
    ground_truth = {} # event_id -> journey_id
    
    start_time = time.time()
    
    for i in range(num_journeys):
        journey_uuid = str(uuid.uuid4())
        
        # Shared key that links all apps in this journey (e.g. a trace_id or global_session_id)
        global_trace_id = f"trace_{journey_uuid}"
        
        apps = [f"app_{k}" for k in range(num_apps)]
        
        for app_idx, app_name in enumerate(apps):
            # App specific session id
            app_session_id = f"session_{journey_uuid}_{app_name}"
            
            for e in range(events_per_app):
                event_id = str(uuid.uuid4())
                
                # Correlation keys logic:
                # 1. Always have the app-specific session ID
                # 2. Some events have the global trace ID to link apps together
                
                c_ids = [app_session_id]
                
                # Link apps together: 
                # First event of each app gets the global trace ID
                if e == 0:
                    c_ids.append(global_trace_id)
                
                # Randomly add some noise/other keys
                if random.random() < 0.1:
                    c_ids.append(f"email_{journey_uuid}@example.com")
                
                event = {
                    "id": event_id,
                    "correlation_ids": c_ids,
                    "payload": {"data": f"payload_{i}_{app_name}_{e}"},
                    "app": app_name
                }
                
                all_events.append(event)
                ground_truth[event_id] = journey_uuid
                
    # Shuffle events to simulate real-world out-of-order ingestion
    random.shuffle(all_events)
    
    print(f"[Generator] Ingesting {len(all_events)} events...")
    
    # Batch ingestion
    BATCH_SIZE = 1000
    for i in range(0, len(all_events), BATCH_SIZE):
        batch = all_events[i:i+BATCH_SIZE]
        jm.ingest_batch(batch)
        
    ingest_time = time.time() - start_time
    print(f"[Generator] Ingestion complete in {ingest_time:.2f} seconds.")
    
    return ground_truth, ingest_time
