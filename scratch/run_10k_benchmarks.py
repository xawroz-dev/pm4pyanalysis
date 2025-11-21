import subprocess
import time
import os
import sys

# Configuration – can be overridden via environment variable BENCH_JOURNEYS
NUM_JOURNEYS = int(os.getenv('BENCH_JOURNEYS', '10000'))
EVENTS_PER_APP = 5
NUM_APPS = 4

# Solutions to benchmark – all graph DB implementations
SOLUTIONS = [
    "networkx",
    "neo4j",
    "memgraph",
    "arangodb",
]

RESULTS_FILE = "benchmarks/results/10K_BENCHMARK_RESULTS.txt"

def get_class_name(name):
    if name == "networkx":
        return "NetworkXJourneyManager"
    if name == "neo4j":
        return "Neo4jJourneyManager"
    if name == "memgraph":
        return "MemgraphJourneyManager"
    if name == "arangodb":
        return "ArangoDBJourneyManager"
    return None

def get_init_args(name):
    if name == "networkx":
        return None
    if name == "neo4j":
        return ["bolt://localhost:7687", "neo4j", "password"]
    if name == "memgraph":
        return ["bolt://localhost:7688", "", ""]
    if name == "arangodb":
        return ["http://localhost:8529", "root", "password"]
    return None

def run_solution(name):
    print(f"\n{'='*60}\nRunning Benchmark: {name.upper()}\n{'='*60}\n")
    solution_dir = os.path.join("benchmarks", "solutions", name)
    has_docker = os.path.exists(os.path.join(solution_dir, "docker-compose.yml"))
    if has_docker:
        print("Starting Docker containers...")
        subprocess.run(["docker-compose", "up", "-d"], cwd=solution_dir, check=True)
        time.sleep(15)  # give DB time to start
    # Build temporary runner script
    runner_code = f"""
import sys, os, time
sys.path.append(os.path.abspath(os.path.join('{solution_dir.replace(os.sep, '/')}','../../'))
from solutions.{name}.main import {get_class_name(name)}
from common.generator import generate_traffic
from common.validator import validate_stitching

args = {get_init_args(name)}
jm = {get_class_name(name)}(*args) if args else {get_class_name(name)}()
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
generated_data, ingest_time = generate_traffic(jm, {NUM_JOURNEYS}, {EVENTS_PER_APP}, {NUM_APPS})
print('Processing events...')
start = time.time()
jm.process_events()
process_time = time.time() - start
print(f"RESULT: Ingestion={{ingest_time:.2f}}s, Processing={{process_time:.2f}}s, Total={{ingest_time+process_time:.2f}}s")
print('Validating stitching...')
validate_stitching(jm, generated_data, sample_size=100)
print('VALIDATION: SUCCESS')
if hasattr(jm, 'close'):
    jm.close()
"""
    try:
        with open('temp_runner.py', 'w') as f:
            f.write(runner_code)
        result = subprocess.run([sys.executable, 'temp_runner.py'], capture_output=True, text=True, timeout=1800)
        print(result.stdout)
        if result.returncode != 0:
            print('Error:', result.stderr)
            return None
        for line in result.stdout.splitlines():
            if line.startswith('RESULT:'):
                return line.replace('RESULT: ', '')
        return None
    finally:
        if has_docker:
            print('Stopping Docker containers...')
            subprocess.run(["docker-compose", "down"], cwd=solution_dir, check=True)

def main():
    results = {}
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    with open(RESULTS_FILE, 'w') as f:
        f.write(f"BENCHMARK RESULTS ({NUM_JOURNEYS} Journeys, {NUM_APPS} Apps, {EVENTS_PER_APP} Events/App = {NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP} Events)\n")
        f.write('='*80 + "\n")
        f.write(f"Test Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write('='*80 + "\n\n")
    for sol in SOLUTIONS:
        print(f"\n{'#'*80}\n# Starting {sol.upper()} Benchmark\n{'#'*80}\n")
        res = run_solution(sol)
        if res:
            results[sol] = res
            with open(RESULTS_FILE, 'a') as f:
                f.write(f"{sol}: {res}\n")
        else:
            with open(RESULTS_FILE, 'a') as f:
                f.write(f"{sol}: FAILED\n")
    print('\n' + '='*80)
    print('BENCHMARK COMPLETE')
    print('='*80)
    print(f"Results saved to: {RESULTS_FILE}")

if __name__ == '__main__':
    main()
