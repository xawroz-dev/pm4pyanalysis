import sys
import os
import argparse
import time
import logging

# Add current directory to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from common.generator import generate_traffic
from common.validator import validate_stitching

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_benchmark(solution_name, num_journeys, events_per_app, num_apps):
    logger.info(f"Starting benchmark for {solution_name}...")
    
    # Dynamic import to avoid name collisions
    import importlib.util
    
    if solution_name == 'cassandra':
        spec = importlib.util.spec_from_file_location("cassandra_main", os.path.join(os.path.dirname(__file__), "cassandra/main.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        JM = module.CassandraJourneyManager
    elif solution_name == 'elasticsearch':
        spec = importlib.util.spec_from_file_location("es_main", os.path.join(os.path.dirname(__file__), "elasticsearch/main.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        JM = module.ElasticsearchJourneyManager
    else:
        raise ValueError(f"Unknown solution: {solution_name}")
        
    jm = JM()
    
    # 1. Setup & Clean
    logger.info("Setting up and cleaning database...")
    jm.setup()
    jm.clean()
    
    # 2. Generate & Ingest
    logger.info("Generating and ingesting traffic...")
    generated_data, duration = generate_traffic(jm, num_journeys, events_per_app, num_apps)
    
    logger.info(f"Ingestion finished in {duration:.2f} seconds.")
    logger.info(f"Throughput: { (num_journeys * events_per_app * num_apps) / duration:.2f} events/sec")
    
    # 3. Process (if async)
    logger.info("Processing events (stitching)...")
    start_proc = time.time()
    jm.process_events()
    proc_duration = time.time() - start_proc
    logger.info(f"Processing finished in {proc_duration:.2f} seconds.")
    
    # 4. Validate
    validate_stitching(jm, generated_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("solution", choices=['cassandra', 'elasticsearch'])
    parser.add_argument("--journeys", type=int, default=2000)
    parser.add_argument("--events", type=int, default=50)
    parser.add_argument("--apps", type=int, default=5)
    
    args = parser.parse_args()
    
    run_benchmark(args.solution, args.journeys, args.events, args.apps)
