# Branch-Aware BPMN Mod Optimizer

A Python optimizer that places data-enrichment mods at optimal positions in BPMN workflows.

## Key Features

- **Branch-aware placement**: Only places mods on branches that need them
- **Nested gateway support**: Properly handles XOR gateways within XOR gateways
- **Mod chaining**: Respects mod dependencies (A→B→C→D chains)
- **Minimal redundancy**: Never calls a mod more than necessary per execution path

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from optimizer import BranchAwareOptimizer

optimizer = BranchAwareOptimizer()
optimizer.parse_bpmn("process.bpmn")

mods = [
    {"name": "ModA", "inputs": ["x"], "outputs": ["y"]},
    {"name": "ModB", "inputs": ["y"], "outputs": ["z"]}
]

placements = optimizer.compute_placements(mods, available_params=["x"])
print(placements)
```

## Running Tests

```bash
python -m pytest tests/ -v
```

## Algorithm

1. Parse BPMN → build graph
2. Topologically sort mods by dependencies
3. For each XOR gateway:
   - Analyze what each branch needs
   - If ALL branches need a mod → place once before gateway
   - If SOME branches need a mod → place on each needing branch
4. Recursively handle nested gateways
