"""
Comprehensive Database Benchmark Runner
Runs all journey stitching solutions and compares performance
"""

import subprocess
import sys
import time
import json
from pathlib import Path
from datetime import datetime

# Configuration
NUM_JOURNEYS = 1000
NUM_APPS = 5
EVENTS_PER_APP = 1
TOTAL_EVENTS = NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP

SOLUTIONS = [
    {
        "name": "Apache AGE",
        "path": "solutions/apache_age/main.py",
        "timeout": 7200,  # 2 hours
        "description": "Graph extension for PostgreSQL"
    },
    {
        "name": "Optimized PostgreSQL",
        "path": "solutions/optimized_postgres/main.py",
        "timeout": 300,  # 5 minutes
        "description": "Hybrid in-memory + PostgreSQL"
    },
    {
        "name": "Neo4j",
        "path": "solutions/neo4j/main.py",
        "timeout": 1800,  # 30 minutes
        "description": "Native graph database"
    },
    {
        "name": "Memgraph",
        "path": "solutions/memgraph/main.py",
        "timeout": 1800,  # 30 minutes
        "description": "In-memory graph database"
    },
    {
        "name": "FalkorDB",
        "path": "solutions/falkordb/main.py",
        "timeout": 1800,  # 30 minutes
        "description": "Redis graph module"
    },
    {
        "name": "ArangoDB",
        "path": "solutions/arangodb/main.py",
        "timeout": 1800,  # 30 minutes
        "description": "Multi-model database"
    },
    {
        "name": "NetworkX",
        "path": "solutions/networkx/main.py",
        "timeout": 3600,  # 1 hour
        "description": "Python graph library"
    }
]

def run_benchmark(solution):
    """Run benchmark for a single solution."""
    print(f"\n{'='*80}")
    print(f"Running: {solution['name']}")
    print(f"Description: {solution['description']}")
    print(f"Configuration: {NUM_JOURNEYS} journeys × {NUM_APPS} apps × {EVENTS_PER_APP} events")
    print(f"Total events: {TOTAL_EVENTS:,}")
    print(f"{'='*80}\n")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, solution['path']],
            capture_output=True,
            text=True,
            timeout=solution['timeout']
        )
        
        elapsed = time.time() - start_time
        
        return {
            "name": solution['name'],
            "description": solution['description'],
            "status": "success" if result.returncode == 0 else "failed",
            "elapsed": elapsed,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
        
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start_time
        return {
            "name": solution['name'],
            "description": solution['description'],
            "status": "timeout",
            "elapsed": elapsed,
            "error": f"Timeout after {solution['timeout']} seconds"
        }
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "name": solution['name'],
            "description": solution['description'],
            "status": "error",
            "elapsed": elapsed,
            "error": str(e)
        }

def extract_metrics(output):
    """Extract performance metrics from output."""
    metrics = {}
    
    for line in output.split('\n'):
        if 'Ingestion Time:' in line:
            try:
                parts = line.split('(')
                if len(parts) > 1:
                    throughput = parts[1].split()[0]
                    metrics['ingestion_throughput'] = float(throughput)
            except:
                pass
        elif 'Processing Time:' in line:
            try:
                parts = line.split('(')
                if len(parts) > 1:
                    throughput = parts[1].split()[0]
                    metrics['processing_throughput'] = float(throughput)
            except:
                pass
        elif 'Total Time:' in line:
            try:
                parts = line.split('(')
                if len(parts) > 1:
                    throughput = parts[1].split()[0]
                    metrics['overall_throughput'] = float(throughput)
                time_part = line.split(':')[1].split('(')[0].strip()
                metrics['total_time'] = float(time_part.replace('s', ''))
            except:
                pass
    
    return metrics

def main():
    """Run all benchmarks and generate report."""
    print(f"\n{'#'*80}")
    print(f"# COMPREHENSIVE DATABASE BENCHMARK SUITE")
    print(f"#")
    print(f"# Configuration:")
    print(f"#   Journeys: {NUM_JOURNEYS:,}")
    print(f"#   Apps per journey: {NUM_APPS}")
    print(f"#   Events per app: {EVENTS_PER_APP}")
    print(f"#   Total events: {TOTAL_EVENTS:,}")
    print(f"#")
    print(f"# Solutions: {len(SOLUTIONS)}")
    print(f"{'#'*80}\n")
    
    results = []
    
    # Run each benchmark
    for solution in SOLUTIONS:
        result = run_benchmark(solution)
        results.append(result)
        
        # Extract metrics
        if result['status'] == 'success':
            metrics = extract_metrics(result['stdout'])
            result['metrics'] = metrics
            
            print(f"\n[OK] {result['name']} completed in {result['elapsed']:.2f}s")
            if metrics:
                print(f"   Throughput: {metrics.get('overall_throughput', 'N/A')} events/sec")
        elif result['status'] == 'timeout':
            print(f"\n[TIMEOUT] {result['name']} timed out after {result['elapsed']:.2f}s")
        else:
            print(f"\n[FAIL] {result['name']} failed: {result.get('error', 'Unknown error')}")
    
    # Generate report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"results/BENCHMARK_COMPARISON_{timestamp}.txt"
    
    Path("results").mkdir(exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write("="*80 + "\n")
        f.write("COMPREHENSIVE DATABASE BENCHMARK RESULTS\n")
        f.write("="*80 + "\n\n")
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Configuration: {NUM_JOURNEYS} journeys × {NUM_APPS} apps × {EVENTS_PER_APP} events = {TOTAL_EVENTS:,} total events\n\n")
        f.write("="*80 + "\n\n")
        
        # Summary table
        f.write("PERFORMANCE SUMMARY\n")
        f.write("="*80 + "\n\n")
        f.write(f"{'Solution':<25} {'Status':<12} {'Time':<12} {'Throughput':<15}\n")
        f.write("-"*80 + "\n")
        
        successful = [r for r in results if r['status'] == 'success']
        successful.sort(key=lambda x: x.get('metrics', {}).get('overall_throughput', 0), reverse=True)
        
        for result in successful:
            metrics = result.get('metrics', {})
            throughput = metrics.get('overall_throughput', 0)
            f.write(f"{result['name']:<25} {'[OK] Success':<12} {result['elapsed']:>8.2f}s   {throughput:>8.0f} events/sec\n")
        
        for result in results:
            if result['status'] != 'success':
                status = '[TIMEOUT] Timeout' if result['status'] == 'timeout' else '[FAIL] Failed'
                f.write(f"{result['name']:<25} {status:<12} {result['elapsed']:>8.2f}s   {'N/A':<15}\n")
        
        f.write("\n" + "="*80 + "\n\n")
        
        # Detailed results
        for result in results:
            f.write(f"\n{'='*80}\n")
            f.write(f"{result['name'].upper()}\n")
            f.write(f"{'='*80}\n\n")
            f.write(f"Description: {result['description']}\n")
            f.write(f"Status: {result['status']}\n")
            f.write(f"Time: {result['elapsed']:.2f}s\n\n")
            
            if result['status'] == 'success':
                metrics = result.get('metrics', {})
                if metrics:
                    f.write("Performance Metrics:\n")
                    f.write(f"  - Ingestion: {metrics.get('ingestion_throughput', 'N/A')} events/sec\n")
                    f.write(f"  - Processing: {metrics.get('processing_throughput', 'N/A')} events/sec\n")
                    f.write(f"  - Overall: {metrics.get('overall_throughput', 'N/A')} events/sec\n")
                    f.write(f"  - Total Time: {metrics.get('total_time', 'N/A')}s\n\n")
                
                f.write("Output:\n")
                f.write("-"*80 + "\n")
                f.write(result['stdout'][-2000:] if len(result['stdout']) > 2000 else result['stdout'])
                f.write("\n" + "-"*80 + "\n")
            else:
                f.write(f"Error: {result.get('error', 'Unknown error')}\n")
                if result.get('stderr'):
                    f.write("\nError Output:\n")
                    f.write("-"*80 + "\n")
                    f.write(result['stderr'][-1000:])
                    f.write("\n" + "-"*80 + "\n")
    
    print(f"\n{'='*80}")
    print(f"Benchmark complete!")
    print(f"Report saved to: {report_path}")
    print(f"{'='*80}\n")
    
    # Print summary
    print("\nPERFORMANCE RANKING:")
    print("-"*80)
    for i, result in enumerate(successful, 1):
        metrics = result.get('metrics', {})
        throughput = metrics.get('overall_throughput', 0)
        print(f"{i}. {result['name']}: {throughput:.0f} events/sec ({result['elapsed']:.2f}s)")

if __name__ == "__main__":
    main()
