import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from common.generator import generate_traffic
from common.validator import validate_stitching

NUM_JOURNEYS = 20000
EVENTS_PER_JOURNEY = 5

print(f"Starting Large-Scale Benchmark: 20,000 journeys × 5 events = 100,000 events")
print("=" * 80)

# NetworkX
print("\n[1/5] Testing NetworkX...")
from solutions.networkx.main import NetworkXJourneyManager
jm = NetworkXJourneyManager()
jm.setup()
jm.clean()

generated_data, ingest_time = generate_traffic(jm, NUM_JOURNEYS, EVENTS_PER_JOURNEY)
start_process = time.time()
jm.process_events()
process_time = time.time() - start_process

print(f"NetworkX - Ingestion: {ingest_time:.2f}s, Processing: {process_time:.2f}s, Total: {ingest_time + process_time:.2f}s")
validate_stitching(jm, generated_data, sample_size=50)

networkx_results = {
    'name': 'NetworkX',
    'ingest': ingest_time,
    'process': process_time,
    'total': ingest_time + process_time
}

# Save results
with open('benchmarks/results/LARGE_SCALE_RESULTS.txt', 'w') as f:
    f.write("=" * 80 + "\n")
    f.write("LARGE-SCALE BENCHMARK RESULTS (20,000 journeys)\n")
    f.write("=" * 80 + "\n\n")
    f.write(f"NetworkX: Ingest={ingest_time:.2f}s, Process={process_time:.2f}s, Total={ingest_time + process_time:.2f}s\n")

print("\n✓ NetworkX complete")
print("\nNote: Persistent database tests will take significantly longer.")
print("Estimated time per solution: 5-15 minutes")
