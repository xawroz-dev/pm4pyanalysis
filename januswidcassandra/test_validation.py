"""Simple test script to verify validation calculations."""
import nest_asyncio
nest_asyncio.apply()

from gremlin_python.driver.client import Client

GREMLIN_URI = 'ws://localhost:8182/gremlin'
USECASE = 'benchmark_demo'

client = Client(GREMLIN_URI, 'g')

try:
    # Count events
    events = client.submit(f"g.V().has('Event', 'usecase', '{USECASE}').count()").all().result()[0]
    print(f"Total Events: {events}")
    
    # Count journeys
    journeys = client.submit(f"g.V().has('Journey', 'usecase', '{USECASE}').count()").all().result()[0]
    print(f"Total Journeys: {journeys}")
    
    # Calculate average
    if journeys > 0:
        avg = events / journeys
        print(f"Average Events per Journey: {avg:.2f}")
        print(f"Expected: 90 (3 apps * 30 events/app)")
    else:
        print("No journeys found - data may have been cleaned up")
        
finally:
    client.close()
