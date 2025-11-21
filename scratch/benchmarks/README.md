# Graph Database Benchmarking for Journey Stitching

This directory contains implementations and benchmarks for 6 different approaches to solving the journey stitching problem.

## Directory Structure

```
benchmarks/
├── common/                    # Shared utilities
│   ├── interface.py          # JourneyManager ABC
│   ├── generator.py          # Traffic generation
│   └── validator.py          # Validation logic
├── solutions/                 # Individual implementations
│   ├── apache_age/
│   ├── neo4j/
│   ├── arangodb/
│   ├── memgraph/
│   ├── postgres_recursive/
│   └── networkx/
└── results/
    └── performance_comparison.txt
```

## Running Individual Benchmarks

Each solution can be run independently:

```powershell
# Apache AGE
cd benchmarks/solutions/apache_age
docker-compose up -d
..\..\..\.venv\Scripts\python.exe main.py
docker-compose down

# Neo4j
cd benchmarks/solutions/neo4j
docker-compose up -d
..\..\..\.venv\Scripts\python.exe main.py
docker-compose down

# ArangoDB
cd benchmarks/solutions/arangodb
docker-compose up -d
..\..\..\.venv\Scripts\python.exe main.py
docker-compose down

# Memgraph
cd benchmarks/solutions/memgraph
docker-compose up -d
..\..\..\.venv\Scripts\python.exe main.py
docker-compose down

# PostgreSQL Recursive
cd benchmarks/solutions/postgres_recursive
docker-compose up -d
..\..\..\.venv\Scripts\python.exe main.py
docker-compose down

# NetworkX (no Docker needed)
cd benchmarks/solutions/networkx
..\..\..\.venv\Scripts\python.exe main.py
```

## Running All Benchmarks

Use the automated runner:

```powershell
.\.venv\Scripts\python.exe run_benchmarks.py
```

## Results

See `benchmarks/results/performance_comparison.txt` for detailed analysis and recommendations.
