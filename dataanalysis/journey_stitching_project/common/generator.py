import time
import uuid

def generate_traffic(jm, num_journeys, events_per_app, num_apps, batch_size=1000):
    """
    Generate traffic for benchmarking journey stitching.
    
    Args:
        jm: JourneyManager instance
        num_journeys: Number of journeys to generate
        events_per_app: Number of events per app per journey
        num_apps: Number of apps generating events
        batch_size: Batch size for ingestion
    
    Returns:
        (generated_data, duration) where generated_data maps journey_index -> list of event_ids
    
    Event correlation strategy:
    - Each journey has a unique base correlation ID
    - Events from the same app within a journey are chained (E1[K1], E2[K1,K2], E3[K2,K3]...)
    - Events from different apps in the same journey share the base correlation ID
    - This ensures all events from the same journey (across all apps) get stitched together
    """
    total_events = num_journeys * num_apps * events_per_app
    print(f"\n[Generator] Generating {num_journeys} journeys with {num_apps} apps and {events_per_app} events per app...")
    print(f"[Generator] Total events to generate: {total_events}")
    
    generated_data = {}  # journey_index -> list of event_ids
    batch_events = []

    start_time = time.time()
    
    for j_idx in range(num_journeys):
        # Base correlation ID shared by all apps for this journey
        journey_base_key = f"journey_{str(uuid.uuid4())[:8]}"
        event_ids = []
        
        # Generate events for each app
        for app_idx in range(num_apps):
            app_name = f"app_{app_idx}"
            
            # App-specific keys for chaining events within the app
            app_keys = [f"{journey_base_key}_{app_name}_{i}" for i in range(events_per_app)]
            
            for event_idx in range(events_per_app):
                e_id = f"event_{journey_base_key}_{app_name}_{event_idx}"
                event_ids.append(e_id)
                
                # Correlation IDs:
                # 1. Journey base key (shared across all apps for this journey)
                # 2. Current app-specific key
                # 3. Previous app-specific key (if not first event in app)
                c_ids = [journey_base_key]  # Always include journey base key
                c_ids.append(app_keys[event_idx])
                if event_idx > 0:
                    c_ids.append(app_keys[event_idx - 1])
                
                batch_events.append({
                    "id": e_id,
                    "correlation_ids": c_ids,
                    "payload": {
                        "app": app_name,
                        "journey_index": j_idx,
                        "event_index": event_idx
                    }
                })
        
        generated_data[j_idx] = event_ids
        
        # Ingest batch if threshold reached
        if len(batch_events) >= batch_size:
            jm.ingest_batch(batch_events)
            batch_events = []

    # Ingest remaining events
    if batch_events:
        jm.ingest_batch(batch_events)

    duration = time.time() - start_time
    print(f"[Generator] Ingestion complete in {duration:.2f} seconds.")
    return generated_data, duration
