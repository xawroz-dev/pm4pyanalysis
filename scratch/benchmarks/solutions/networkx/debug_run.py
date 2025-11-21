import sys
import os
import time

# Add scratch directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from solutions.networkx.main import NetworkXJourneyManager
from common.generator import generate_traffic
from common.validator import validate_stitching

print("Initializing NetworkX...")
jm = NetworkXJourneyManager()
jm.setup()
jm.clean()

NUM_JOURNEYS = 20000
EVENTS_PER_APP = 5
NUM_APPS = 4

print(f"Generating traffic: {NUM_JOURNEYS} journeys, {NUM_APPS} apps, {EVENTS_PER_APP} events/app...")
generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_APP, NUM_APPS)

print("Processing events...")
start = time.time()
jm.process_events()
process_time = time.time() - start

print(f"RESULT: Ingestion={ingest_time:.2f}s, Processing={process_time:.2f}s, Total={ingest_time+process_time:.2f}s")

validate_stitching(jm, generated_data, sample_size=50)
