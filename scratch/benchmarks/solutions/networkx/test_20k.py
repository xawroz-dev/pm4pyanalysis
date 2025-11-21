import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.generator import generate_traffic
from common.validator import validate_stitching
from solutions.networkx.main import NetworkXJourneyManager

NUM_JOURNEYS = 20000
EVENTS_PER_JOURNEY = 5

print(f"NetworkX Large-Scale Test: {NUM_JOURNEYS} journeys × {EVENTS_PER_JOURNEY} events")

jm = NetworkXJourneyManager()
jm.setup()
jm.clean()

generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_JOURNEY)

start_process = time.time()
jm.process_events()
process_time = time.time() - start_process

total_time = ingest_time + process_time

print(f"\nResults:")
print(f"  Ingestion: {ingest_time:.2f}s")
print(f"  Processing: {process_time:.2f}s")
print(f"  Total: {total_time:.2f}s")

validate_stitching(jm, generated_data, sample_size=50)

# Save results
with open('../../results/networkx_20k.txt', 'w') as f:
    f.write(f"NetworkX - 20,000 journeys\n")
    f.write(f"Ingestion: {ingest_time:.2f}s\n")
    f.write(f"Processing: {process_time:.2f}s\n")
    f.write(f"Total: {total_time:.2f}s\n")
