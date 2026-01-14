import os
import argparse
from gremlin_python.driver.client import Client

# Configuration
GREMLIN_URI = os.environ.get('GREMLIN_URI', 'ws://localhost:8182/gremlin')

def validate(args):
    client = Client(GREMLIN_URI, 'g')
    usecase = args.usecase
    
    print(f"Validating Usecase: {usecase}")
    
    # 1. Count Total Events
    count_events = client.submit(f"g.V().has('Event', 'usecase', '{usecase}').count()").all().result()[0]
    print(f"Total Events: {count_events}")
    
    # 2. Count Total Journeys
    count_journeys = client.submit(f"g.V().has('Journey', 'usecase', '{usecase}').count()").all().result()[0]
    print(f"Total Journeys: {count_journeys}")
    
    # 3. Verify Events per Journey
    # We expect roughly (events_per_app * apps) events per journey
    # Let's sample or check average
    
    avg_events = client.submit(f"g.V().has('Journey', 'usecase', '{usecase}').in('PART_OF_JOURNEY').count().mean()").all().result()[0]
    print(f"Average Events per Journey: {avg_events}")
    
    # 4. Check for Orphaned Events (Events without Journey)
    orphans = client.submit(f"g.V().has('Event', 'usecase', '{usecase}').where(__.not(__.out('PART_OF_JOURNEY'))).count()").all().result()[0]
    print(f"Orphaned Events (Unstitched): {orphans}")
    
    if orphans == 0 and count_journeys > 0:
        print("SUCCESS: All events stitched.")
    else:
        print("FAILURE: Found orphaned events or no journeys.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='JanusGraph Validator')
    parser.add_argument('--usecase', type=str, default='production_case', help='Usecase identifier')
    
    args = parser.parse_args()
    validate(args)
