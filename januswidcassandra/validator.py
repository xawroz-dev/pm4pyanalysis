import os
import argparse

# Fix Windows asyncio compatibility
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass

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
    
    # 3. Calculate Average Events per Journey
    # Correct calculation: Total Events / Total Journeys
    if count_journeys > 0:
        avg_events = count_events / count_journeys
        print(f"Average Events per Journey: {avg_events:.2f}")
    else:
        print("Average Events per Journey: N/A (no journeys)")
    
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
