import subprocess
import time
import os

solutions = [
    {"name": "Apache AGE", "path": "benchmarks/solutions/apache_age"},
    {"name": "Neo4j", "path": "benchmarks/solutions/neo4j"},
    {"name": "ArangoDB", "path": "benchmarks/solutions/arangodb"},
    {"name": "Memgraph", "path": "benchmarks/solutions/memgraph"},
    {"name": "PostgreSQL Recursive", "path": "benchmarks/solutions/postgres_recursive"},
    {"name": "NetworkX", "path": "benchmarks/solutions/networkx"}
]

results = []

print("=" * 80)
print("BENCHMARK RUNNER - Journey Stitching Performance Comparison")
print("=" * 80)

for solution in solutions:
    print(f"\n{'='*80}")
    print(f"Running: {solution['name']}")
    print(f"{'='*80}")
    
    # Start docker if docker-compose exists
    docker_compose_path = os.path.join(solution['path'], 'docker-compose.yml')
    if os.path.exists(docker_compose_path):
        print(f"Starting Docker containers for {solution['name']}...")
        subprocess.run(['docker-compose', 'up', '-d'], cwd=solution['path'], shell=True)
        time.sleep(10)  # Wait for container to be ready
    
    # Run the benchmark
    try:
        result = subprocess.run(
            [r'c:\Users\admin\.gemini\antigravity\scratch\.venv\Scripts\python.exe', 'main.py'],
            cwd=solution['path'],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        output = result.stdout
        print(output)
        
        # Parse results
        ingest_time = None
        process_time = None
        total_time = None
        validation = "UNKNOWN"
        
        for line in output.split('\n'):
            if 'Ingestion Time:' in line:
                ingest_time = line.split(':')[1].strip()
            elif 'Processing Time:' in line:
                process_time = line.split(':')[1].strip()
            elif 'Total Time:' in line:
                total_time = line.split(':')[1].strip()
            elif 'passed' in line.lower():
                validation = "PASSED" if "20/20" in line else "FAILED"
        
        results.append({
            'name': solution['name'],
            'ingest_time': ingest_time or 'N/A',
            'process_time': process_time or 'N/A',
            'total_time': total_time or 'N/A',
            'validation': validation
        })
        
    except Exception as e:
        print(f"Error running {solution['name']}: {e}")
        results.append({
            'name': solution['name'],
            'ingest_time': 'ERROR',
            'process_time': 'ERROR',
            'total_time': 'ERROR',
            'validation': 'ERROR'
        })
    
    # Stop docker
    if os.path.exists(docker_compose_path):
        print(f"Stopping Docker containers for {solution['name']}...")
        subprocess.run(['docker-compose', 'down'], cwd=solution['path'], shell=True)

# Generate summary
print("\n" + "=" * 80)
print("BENCHMARK RESULTS SUMMARY")
print("=" * 80)
print(f"{'Solution':<25} {'Ingest':<12} {'Process':<12} {'Total':<12} {'Validation'}")
print("-" * 80)

for r in results:
    print(f"{r['name']:<25} {r['ingest_time']:<12} {r['process_time']:<12} {r['total_time']:<12} {r['validation']}")

# Save to file
with open('benchmarks/results/performance_comparison.txt', 'w') as f:
    f.write("=" * 80 + "\n")
    f.write("Journey Stitching Performance Comparison\n")
    f.write("Test: 1000 journeys, 5 events each (5000 total events)\n")
    f.write("=" * 80 + "\n\n")
    
    f.write(f"{'Solution':<25} {'Ingest':<12} {'Process':<12} {'Total':<12} {'Validation'}\n")
    f.write("-" * 80 + "\n")
    
    for r in results:
        f.write(f"{r['name']:<25} {r['ingest_time']:<12} {r['process_time']:<12} {r['total_time']:<12} {r['validation']}\n")

print("\nResults saved to: benchmarks/results/performance_comparison.txt")
