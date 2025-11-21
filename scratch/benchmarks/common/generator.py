import time
import uuid

def generate_traffic(jm, num_journeys, events_per_journey, batch_size=1000):
    print(f"\n[Generator] Generating {num_journeys} journeys with {events_per_journey} events each...")
    
    generated_data = {} # journey_index -> list of event_ids
    batch_events = []

    start_time = time.time()
    
    for j_idx in range(num_journeys):
        base_key = str(uuid.uuid4())[:8]
        event_ids = []
        
        # Chain: E1[K1], E2[K1, K2], E3[K2, K3]...
        keys = [f"key_{base_key}_{i}" for i in range(events_per_journey)]
        
        for i in range(events_per_journey):
            e_id = f"event_{base_key}_{i}"
            event_ids.append(e_id)
            
            c_ids = []
            c_ids.append(keys[i])
            if i > 0:
                c_ids.append(keys[i-1])
            
            batch_events.append({
                "id": e_id,
                "correlation_ids": c_ids,
                "payload": {}
            })
            
        generated_data[j_idx] = event_ids
        
        if len(batch_events) >= batch_size:
            jm.ingest_batch(batch_events)
            batch_events = []
            # print(f"  -> Ingested batch... (Total journeys: {j_idx + 1})")

    if batch_events:
        jm.ingest_batch(batch_events)

    duration = time.time() - start_time
    print(f"[Generator] Ingestion complete in {duration:.2f} seconds.")
    return generated_data, duration
