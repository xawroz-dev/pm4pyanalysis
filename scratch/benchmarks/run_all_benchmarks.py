import importlib.util
import os
import sys
import subprocess
from pathlib import Path

# Directories
BASE_DIR = Path(__file__).resolve().parent
SOLUTIONS_DIR = BASE_DIR / "solutions"
RESULTS_DIR = BASE_DIR / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_PATH = RESULTS_DIR / "SCALABILITY_ANALYSIS.txt"

# List of solution directories to benchmark (excluding postgres_recursive)
SOLUTIONS = [
    "apache_age",
    "arangodb",
    "memgraph",
    "neo4j",
    "networkx",
]

def load_main(module_path: str):
    spec = importlib.util.spec_from_file_location("main", module_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def run_solution(name: str) -> str:
    sol_dir = SOLUTIONS_DIR / name
    main_path = sol_dir / "main.py"
    if not main_path.is_file():
        raise FileNotFoundError(f"main.py not found for solution {name}")
    # Try to import and call a run_benchmark function if present
    mod = load_main(str(main_path))
    if hasattr(mod, "run_benchmark"):
        return mod.run_benchmark()
    # Fallback: execute the script as a subprocess (it runs its own benchmark when __main__)
    result = subprocess.run([sys.executable, str(main_path)], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Error running {name}: {result.stderr}")
    return result.stdout

def main():
    with open(RESULTS_PATH, "w", encoding="utf-8") as out:
        for sol in SOLUTIONS:
            out.write(f"=== {sol.upper()} ===\n")
            try:
                output = run_solution(sol)
                out.write(output + "\n")
            except Exception as e:
                out.write(f"Error running {sol}: {e}\n")
            out.write("\n")
    print(f"Benchmark results written to {RESULTS_PATH}")

if __name__ == "__main__":
    main()
