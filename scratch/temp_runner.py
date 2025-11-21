
import sys, os, time
sys.path.append(os.path.abspath(os.path.join('benchmarks/solutions/arangodb','../../'))
from solutions.arangodb.main import ArangoDBJourneyManager
from common.generator import generate_traffic
from common.validator import validate_stitching

args = ['http://localhost:8529', 'root', 'password']
jm = ArangoDBJourneyManager(*args) if args else ArangoDBJourneyManager()
# Wait for DB / connection
if hasattr(jm, 'connect'):
    for _ in range(30):
        try:
            jm.connect()
            break
        except Exception:
            time.sleep(2)
elif hasattr(jm, 'setup'):
    for _ in range(30):
        try:
            jm.setup()
            break
        except Exception:
            time.sleep(2)

jm.setup()
jm.clean()
print('Generating traffic...')
generated_data, ingest_time = generate_traffic(jm, 1000, 5, 4)
print('Processing events...')
start = time.time()
jm.process_events()
process_time = time.time() - start
print(f"RESULT: Ingestion={ingest_time:.2f}s, Processing={process_time:.2f}s, Total={ingest_time+process_time:.2f}s")
print('Validating stitching...')
validate_stitching(jm, generated_data, sample_size=100)
print('VALIDATION: SUCCESS')
if hasattr(jm, 'close'):
    jm.close()
