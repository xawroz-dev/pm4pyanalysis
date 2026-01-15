import time
import uuid
import os
import argparse
from gremlin_python.driver.client import Client

# Configuration
GREMLIN_URI = os.environ.get('GREMLIN_URI', 'ws://localhost:8182/gremlin')

# Optimized Groovy script - pre-create correlation keys, then bulk add events
BATCH_INGEST_SCRIPT = """
def ingestBatch(events, usecase, correlationKeys) {
    def g = graph.traversal()
    def keyCache = [:]
    
    // Pre-create all correlation keys first (idempotent)
    correlationKeys.each { key ->
        def v_corr = g.V().has('CorrelationKey', 'usecase', usecase)
            .has('correlationValue', key)
            .fold()
            .coalesce(
                __.unfold(), 
                __.addV('CorrelationKey')
                  .property('usecase', usecase)
                  .property('correlationValue', key)
            ).next()
        keyCache[key] = v_corr
    }
    
    // Now add all events and link to cached keys
    events.each { event ->
        def v_event = g.addV('Event')
            .property('usecase', usecase)
            .property('eventId', event.eventId)
            .property('timestamp', event.timestamp)
            .property('activityName', event.activityName)
            .property('appName', event.appName)
            .property('correlationValue', event.correlationValue)
            .next()
            
        def v_corr = keyCache[event.correlationValue]
        if (v_corr != null) {
            g.V(v_event).addE('HAS_CORRELATION').to(v_corr).iterate()
        }
    }
    graph.tx().commit()
    return "Batch processed"
}
ingestBatch(events, usecase, correlationKeys)
"""

class EventGenerator:
    def __init__(self, num_journeys, num_apps, events_per_app, usecase):
        self.num_journeys = num_journeys
        self.num_apps = num_apps
        self.events_per_app = events_per_app
        self.usecase = usecase
        self.apps = [f"App_{i}" for i in range(num_apps)]
        
    def generate_all(self):
        all_events = []
        for j_idx in range(self.num_journeys):
            keys = []
            for k in range(self.num_apps - 1):
                keys.append(f"corr_{k}_{k+1}_{j_idx}")
            
            if not keys:
                keys.append(f"corr_self_{j_idx}")

            for app_idx, app_name in enumerate(self.apps):
                app_keys = []
                if app_idx < len(keys):
                    app_keys.append(keys[app_idx])
                if app_idx > 0 and (app_idx - 1) < len(keys):
                    app_keys.append(keys[app_idx - 1])
                
                app_keys = list(set(app_keys))
                
                for i in range(self.events_per_app):
                    c_val = app_keys[i % len(app_keys)]
                    
                    all_events.append({
                        "eventId": str(uuid.uuid4()),
                        "timestamp": int(time.time() * 1000) + i,
                        "activityName": f"Activity_{app_name}_{i}",
                        "appName": app_name,
                        "correlationValue": c_val
                    })
        return all_events

def ingest(args):
    client = Client(GREMLIN_URI, 'g')
    
    print(f"Generating data for Usecase: {args.usecase}")
    generator = EventGenerator(args.journeys, args.apps, args.events_per_app, args.usecase)
    events = generator.generate_all()
    print(f"Generated {len(events)} events.")
    
    start_time = time.time()
    
    batch_size = args.batch_size
    total_batches = (len(events) + batch_size - 1) // batch_size
    
    print(f"Ingesting in {total_batches} batches of size {batch_size}...")
    
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        # Extract unique correlation keys from this batch
        correlation_keys = list(set(e["correlationValue"] for e in batch))
        
        try:
            bindings = {
                'events': batch,
                'usecase': args.usecase,
                'correlationKeys': correlation_keys
            }
            client.submit(BATCH_INGEST_SCRIPT, bindings).all().result()
            
            if (i // batch_size) % 10 == 0:
                print(f"Processed batch {i // batch_size + 1}/{total_batches}")
                
        except Exception as e:
            print(f"Error in batch {i}: {e}")
            
    end_time = time.time()
    duration = end_time - start_time
    print(f"Ingestion completed in {duration:.2f}s")
    print(f"Throughput: {len(events) / duration:.2f} events/s")
    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='JanusGraph Ingestor')
    parser.add_argument('--usecase', type=str, default='production_case', help='Usecase identifier')
    parser.add_argument('--journeys', type=int, default=1000, help='Number of journeys')
    parser.add_argument('--apps', ty pe=int, default=3, help='Number of apps per journey')
    parser.add_argument('--events_per_app', type=int, default=30, help='Events per app')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size')
    
    args = parser.parse_args()
    ingest(args)
