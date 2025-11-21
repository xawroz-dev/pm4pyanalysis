import sys
import os
import time

# Add scratch directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from solutions.apache_age.main import ApacheAgeJourneyManager
from common.generator import generate_traffic
from common.validator import validate_stitching

RESULTS_FILE = "../../results/FINAL_BENCHMARK_RESULTS.txt"

print("Initializing Apache AGE...")
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5436
}
jm = ApacheAgeJourneyManager(DB_CONFIG)

# Wait for DB
for i in range(30):
    try:
        jm.connect()
        break
    except:
        time.sleep(2)

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

result_str = f"Ingestion={ingest_time:.2f}s, Processing={process_time:.2f}s, Total={ingest_time+process_time:.2f}s"
print(f"RESULT: {result_str}")

# Append to results file
# We need to replace the FAILED line if it exists, or append
# Since the main runner wrote FAILED, we should probably just append a new line for now and I'll clean it up later.
with open(RESULTS_FILE, "a") as f:
    f.write(f"apache_age (retry): {result_str}\n")

validate_stitching(jm, generated_data, sample_size=50)
