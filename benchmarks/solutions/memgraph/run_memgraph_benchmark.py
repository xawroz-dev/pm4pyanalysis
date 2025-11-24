import sys
import os
import time

# Add scratch directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from solutions.memgraph.main import MemgraphJourneyManager
from common.generator import generate_traffic
from common.validator import validate_stitching

RESULTS_FILE = "../../results/FINAL_BENCHMARK_RESULTS.txt"

print("Initializing Memgraph...")
jm = MemgraphJourneyManager("bolt://localhost:7688", "", "")

# Wait for DB
for i in range(30):
    try:
        jm.setup()
        break
    except:
        time.sleep(2)

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

result_str = f"Ingestion={ingest_time:.2f}s, Processing={process_time:.2f}s, Total={ingest_time+process_time:.2f}s"
print(f"RESULT: {result_str}")

# Append to results file
with open(RESULTS_FILE, "a") as f:
    f.write(f"memgraph: {result_str}\n")

validate_stitching(jm, generated_data, sample_size=50)
jm.close()
