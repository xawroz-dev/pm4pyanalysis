"""
Simple benchmark script for Apache AGE with 150,000 events
Configuration: 1000 journeys × 5 apps × 30 events/app
"""

import sys
import os
import time

# Add path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Import from the backup file
from solutions.apache_age import main as age_main_backup

# Configuration
NUM_JOURNEYS = 1000
NUM_APPS = 5
EVENTS_PER_APP = 30

print(f"\n{'='*80}")
print(f"APACHE AGE BENCHMARK")
print(f"{'='*80}")
print(f"Configuration:")
print(f"  - Journeys: {NUM_JOURNEYS:,}")
print(f"  - Apps: {NUM_APPS}")
print(f"  - Events per app: {EVENTS_PER_APP}")
print(f"  - Total events: {NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP:,}")
print(f"{'='*80}\n")

# Run benchmark
start = time.time()

try:
    # This will execute the main block from the module
    exec(open('solutions/apache_age/main copy.py').read())
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

elapsed = time.time() - start
print(f"\nTotal benchmark time: {elapsed:.2f}s")
