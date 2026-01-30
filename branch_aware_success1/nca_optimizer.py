"""
NCA-Based BPMN Mod Optimizer

Algorithm:
1. Parse BPMN → convert to directed graph
2. Find all paths from start to end nodes
3. For each mod (in dependency order):
   a. Find all nodes that need this mod (directly or indirectly)
   b. Find the Nearest Common Ancestor (NCA) of these nodes
   c. From NCA, check if ALL paths from NCA that reach end use this mod
   d. If yes → place mod before NCA
   e. If no → recursively apply same logic for each path from NCA

Key insight: NCA is the deepest node that is an ancestor of ALL nodes needing the mod.
"""

import random
import string
from collections import defaultdict
from typing import Dict, List, Set, Optional, Tuple
import networkx as nx
from lxml import etree


# =============================================================================
# CONSTANTS
# =============================================================================

BPMN_NAMESPACES = {
    'bpmn': 'http://www.omg.org/spec/BPMN/20100524/MODEL',
    'camunda': 'http://camunda.org/schema/1.0/bpmn',
}


def generate_task_id() -> str:
    """Generate a unique task ID: Task_xxxxxxx"""
    chars = string.ascii_lowercase + string.digits
    return f"Task_{''.join(random.choices(chars, k=7))}"


# =============================================================================
# NCA OPTIMIZER
# =============================================================================

class NCAOptimizer:
    """
    Optimizer using Nearest Common Ancestor (NCA) algorithm.
    
    For each mod:
    1. Find all nodes needing this mod
    2. Find NCA of those nodes
    3. Check if all paths from NCA need the mod
    4. If yes: place before NCA
    5. If no: recursively process each sub-path
    """
    
    def __init__(self):
        self.graph: Optional[nx.DiGraph] = None
        self.start_node: Optional[str] = None
        self.end_nodes: Set[str] = set()
        self.all_paths: List[List[str]] = []
        self.output_to_mod: Dict[str, str] = {}
        self.sorted_mods: List[Dict] = []
        self.placements: List[Dict] = []
        self.available_at_node: Dict[str, Set[str]] = {}  # node -> available params
        # Track the last mod placed before each node for chaining
        self.last_mod_before: Dict[str, str] = {}  # target_node -> last_mod_id
        
    # -------------------------------------------------------------------------
    # BPMN Parsing
    # -------------------------------------------------------------------------
    
    def parse_bpmn(self, bpmn_path: str) -> nx.DiGraph:
        """Parse BPMN file and build graph."""
        with open(bpmn_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return self.parse_bpmn_string(content)
    
    def parse_bpmn_string(self, bpmn_xml: str) -> nx.DiGraph:
        """Parse BPMN XML string."""
        root = etree.fromstring(bpmn_xml.encode('utf-8'))
        process = root.find('.//bpmn:process', BPMN_NAMESPACES)
        
        if process is None:
            raise ValueError("No process element found in BPMN")
        
        self.graph = nx.DiGraph()
        self.end_nodes = set()
        
        node_types = [
            'startEvent', 'endEvent', 'serviceTask', 'task', 'userTask',
            'exclusiveGateway', 'parallelGateway', 'inclusiveGateway'
        ]
        
        for node_type in node_types:
            for elem in process.findall(f'.//bpmn:{node_type}', BPMN_NAMESPACES):
                node_id = elem.get('id')
                name = elem.get('name', '')
                
                inputs = set()
                input_output = elem.find('.//camunda:inputOutput', BPMN_NAMESPACES)
                if input_output is not None:
                    for param in input_output.findall('camunda:inputParameter', BPMN_NAMESPACES):
                        param_name = param.get('name')
                        if param_name:
                            inputs.add(param_name)
                
                self.graph.add_node(
                    node_id,
                    type=node_type,
                    name=name,
                    inputs=inputs
                )
                
                if node_type == 'startEvent':
                    self.start_node = node_id
                elif node_type == 'endEvent':
                    self.end_nodes.add(node_id)
        
        for flow in process.findall('.//bpmn:sequenceFlow', BPMN_NAMESPACES):
            source = flow.get('sourceRef')
            target = flow.get('targetRef')
            if source in self.graph and target in self.graph:
                self.graph.add_edge(source, target)
        
        return self.graph
    
    # -------------------------------------------------------------------------
    # Path Finding
    # -------------------------------------------------------------------------
    
    def find_all_paths(self) -> List[List[str]]:
        """Find all paths from start to any end node."""
        self.all_paths = []
        
        for end_node in self.end_nodes:
            paths = list(nx.all_simple_paths(self.graph, self.start_node, end_node))
            self.all_paths.extend(paths)
        
        print(f"\n[PATHS] Found {len(self.all_paths)} paths:")
        for i, path in enumerate(self.all_paths):
            print(f"  Path {i+1}: {' -> '.join(path)}")
        
        return self.all_paths
    
    # -------------------------------------------------------------------------
    # NCA Finding
    # -------------------------------------------------------------------------
    
    def find_ancestors(self, node: str) -> Set[str]:
        """Find all ancestors of a node (nodes that can reach it)."""
        return nx.ancestors(self.graph, node)
    
    def find_nca(self, nodes: Set[str]) -> Optional[str]:
        """
        Find the Nearest Common Ancestor of a set of nodes.
        
        NCA is the deepest node that is an ancestor of ALL given nodes.
        Depth is measured as distance from start node.
        """
        if not nodes:
            return None
        
        if len(nodes) == 1:
            node = list(nodes)[0]
            # Return the predecessor of this single node
            preds = list(self.graph.predecessors(node))
            return preds[0] if preds else self.start_node
        
        # Get ancestors for each node
        ancestor_sets = []
        for node in nodes:
            ancestors = self.find_ancestors(node)
            ancestors.add(node)  # A node is its own ancestor
            ancestor_sets.append(ancestors)
        
        # Common ancestors = intersection of all ancestor sets
        common_ancestors = set.intersection(*ancestor_sets)
        
        if not common_ancestors:
            return self.start_node
        
        # Find the deepest common ancestor (furthest from start)
        depths = {}
        for ancestor in common_ancestors:
            try:
                depth = nx.shortest_path_length(self.graph, self.start_node, ancestor)
                depths[ancestor] = depth
            except nx.NetworkXNoPath:
                depths[ancestor] = 0
        
        # Return the deepest one
        nca = max(depths.keys(), key=lambda x: depths[x])
        return nca
    
    # -------------------------------------------------------------------------
    # Mod Dependency Resolution
    # -------------------------------------------------------------------------
    
    def topological_sort_mods(self, mods: List[Dict], available_params: Set[str]) -> List[Dict]:
        """
        Sort mods by dependencies (topological order).
        
        IMPORTANT: Only creates dependencies for inputs that are NOT already available.
        This prevents false cycles when params are pre-available.
        
        Example: If ModA needs 'x' and ModC produces 'x', but 'x' is already available,
        then ModA doesn't actually depend on ModC.
        """
        dep_graph = {m['name']: set() for m in mods}
        
        for mod in mods:
            for inp in mod.get('inputs', []):
                # Only create dependency if input is NOT already available
                if inp not in available_params and inp in self.output_to_mod:
                    producer = self.output_to_mod[inp]
                    if producer != mod['name']:
                        dep_graph[mod['name']].add(producer)
        
        in_degree = {name: len(deps) for name, deps in dep_graph.items()}
        ready = [name for name, deg in in_degree.items() if deg == 0]
        sorted_names = []
        
        dependents = defaultdict(set)
        for mod_name, deps in dep_graph.items():
            for dep in deps:
                dependents[dep].add(mod_name)
        
        while ready:
            current = ready.pop(0)
            sorted_names.append(current)
            for dependent in dependents[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    ready.append(dependent)
        
        # Check for remaining mods (cycle detected)
        if len(sorted_names) != len(mods):
            remaining = set(m['name'] for m in mods) - set(sorted_names)
            print(f"  [WARNING] Cycle detected in mod dependencies: {remaining}")
            # Add remaining mods in original order
            for mod in mods:
                if mod['name'] not in sorted_names:
                    sorted_names.append(mod['name'])
        
        name_to_mod = {m['name']: m for m in mods}
        return [name_to_mod[name] for name in sorted_names]
    
    def get_nodes_needing_mod(self, mod: Dict, available: Set[str]) -> Set[str]:
        """
        Find all nodes that need this mod's outputs (directly OR indirectly).
        
        Indirect means: if NodeX needs paramZ, and paramZ comes from ModC,
        and ModC needs paramY from ModB, and ModB needs paramX from this mod,
        then NodeX indirectly needs this mod.
        """
        mod_outputs = set(mod.get('outputs', []))
        needing_nodes = set()
        
        for node_id in self.graph.nodes():
            node_inputs = self.graph.nodes[node_id].get('inputs', set())
            
            # What params does this node need that aren't available?
            needed_params = node_inputs - available
            
            if not needed_params:
                continue
            
            # Check if this mod provides any needed param (directly)
            if needed_params & mod_outputs:
                needing_nodes.add(node_id)
                continue
            
            # Check INDIRECTLY: does any mod that provides what we need
            # depend on THIS mod's outputs?
            for needed_param in needed_params:
                if self._param_depends_on_mod(needed_param, mod, available):
                    needing_nodes.add(node_id)
                    break
        
        return needing_nodes
    
    def _param_depends_on_mod(
        self, 
        param: str, 
        target_mod: Dict, 
        available: Set[str],
        visited: Optional[Set[str]] = None
    ) -> bool:
        """
        Check if getting `param` requires `target_mod` (directly or transitively).
        
        Example: param=emailStatus, target_mod=GetAccountDetails
        - emailStatus comes from SendEmail
        - SendEmail needs emailId
        - emailId comes from GetAccountDetails
        → So emailStatus depends on GetAccountDetails
        """
        if visited is None:
            visited = set()
        
        if param in visited:
            return False  # Circular dependency
        visited.add(param)
        
        # If param is already available, no mod needed
        if param in available:
            return False
        
        # Check if target_mod produces this param
        if param in set(target_mod.get('outputs', [])):
            return True
        
        # Find which mod produces this param
        producing_mod_name = self.output_to_mod.get(param)
        if not producing_mod_name:
            return False
        
        # Get the producing mod
        producing_mod = next(
            (m for m in self.sorted_mods if m['name'] == producing_mod_name), 
            None
        )
        if not producing_mod:
            return False
        
        # Check if producing mod's inputs depend on target_mod
        for input_param in producing_mod.get('inputs', []):
            if self._param_depends_on_mod(input_param, target_mod, available, visited):
                return True
        
        return False
    
    # -------------------------------------------------------------------------
    # Main Algorithm
    # -------------------------------------------------------------------------
    
    def compute_placements(
        self, 
        mods: List[Dict], 
        available_params: List[str]
    ) -> List[Dict]:
        """
        Compute optimal placements using NCA algorithm.
        
        For each mod:
        1. Find nodes needing this mod
        2. Find NCA of those nodes
        3. If all paths from NCA need mod → place before NCA
        4. Else → recursively process sub-paths
        """
        # Build output mapping
        self.output_to_mod = {}
        for mod in mods:
            for output in mod.get('outputs', []):
                self.output_to_mod[output] = mod['name']
        
        # Sort mods by dependency (considering available params to avoid false cycles)
        self.sorted_mods = self.topological_sort_mods(mods, set(available_params))
        
        # Find all paths
        self.find_all_paths()
        
        # Track available params at each point
        global_available = set(available_params)
        
        # Reset placement tracking
        self.last_mod_before = {}
        
        # Process mods in order
        self.placements = []
        
        for mod in self.sorted_mods:
            print(f"\n[PROCESSING MOD] {mod['name']}")
            print(f"  Inputs: {mod.get('inputs', [])}")
            print(f"  Outputs: {mod.get('outputs', [])}")
            
            self._place_mod_nca(mod, global_available.copy())
            
            # Update global available (mod outputs now available where placed)
            # Note: This is simplified; in reality, availability is path-dependent
        
        # Validate and fix any circular references in placements
        self._validate_and_fix_placements()
        
        return self.placements
    
    def _validate_and_fix_placements(self):
        """
        Validate placements and fix any circular references.
        
        A circular reference happens when:
        - ModA.before = ModB
        - ModB.before = ModC
        - ModC.before = ModA  (loop!)
        
        This can occur when mods are placed independently without proper chaining.
        """
        print("\n[VALIDATING PLACEMENTS]")
        
        # Build a graph of before/after relationships
        # modId -> before_modId
        before_map = {}
        after_map = {}
        
        for p in self.placements:
            mod_id = p['modId']
            before_map[mod_id] = p['before']
            after_map[mod_id] = p['after']
        
        # Check for cycles using DFS
        def has_cycle(start_id, visited, path):
            """Check if following 'before' links creates a cycle."""
            if start_id in path:
                return True, path[path.index(start_id):]
            if start_id in visited:
                return False, []
            if start_id not in before_map:
                return False, []
            
            visited.add(start_id)
            path.append(start_id)
            
            next_id = before_map[start_id]
            # Only continue if next_id is also a mod (not a BPMN node)
            if next_id in before_map:
                return has_cycle(next_id, visited, path)
            
            path.pop()
            return False, []
        
        # Find cycles
        all_mod_ids = set(before_map.keys())
        cycles_found = []
        
        for mod_id in all_mod_ids:
            is_cycle, cycle_path = has_cycle(mod_id, set(), [])
            if is_cycle and cycle_path not in cycles_found:
                cycles_found.append(cycle_path)
                print(f"  [WARNING] Cycle detected: {' -> '.join(cycle_path + [cycle_path[0]])}")
        
        if not cycles_found:
            print("  ✓ No circular references found")
            return
        
        # Fix cycles by breaking them
        # Strategy: Find where the cycle happens and redirect to the original BPMN target
        for cycle in cycles_found:
            print(f"  Fixing cycle: {cycle}")
            
            # The cycle is: mod1 -> mod2 -> ... -> modN -> mod1
            # We need to find a mod in the cycle whose 'before' should be a BPMN node, not another mod
            
            # Find the first mod in the cycle whose 'after' is a BPMN node (not another mod)
            # That mod's chain should end at a BPMN node
            
            for i, mod_id in enumerate(cycle):
                after_node = after_map.get(mod_id)
                
                # If 'after' is a BPMN node (not a mod), this is the start of a chain
                if after_node and after_node not in before_map:
                    # This mod's chain should go: after_node -> mod -> ... -> BPMN_target
                    # Find the end of this chain (should be the original target)
                    
                    # Look for what BPMN node this chain was targeting
                    # by following the 'before' links until we hit a BPMN node
                    # But if we hit a cycle, we need to break it
                    
                    # Find the last mod before the cycle loops back
                    prev_mod_idx = (i - 1) % len(cycle)
                    prev_mod_id = cycle[prev_mod_idx]
                    
                    # The previous mod's 'before' is creating the cycle
                    # It should point to the original target, not back to mod_id
                    
                    # Find the original target by looking at what 'after' chain this belongs to
                    for p in self.placements:
                        if p['modId'] == prev_mod_id:
                            # Get the original target from last_mod_before tracking
                            # The target should be found by looking at current mod's chain
                            for target, last_mod in self.last_mod_before.items():
                                if last_mod == prev_mod_id and target not in before_map:
                                    # Found the original BPMN target
                                    old_before = p['before']
                                    p['before'] = target
                                    print(f"    Fixed: {prev_mod_id}.before: {old_before} -> {target}")
                                    break
                            break
                    break
        
        print("  ✓ Cycles fixed")
    
    def _place_mod_nca(
        self, 
        mod: Dict, 
        available: Set[str],
        scope_start: Optional[str ] = None
    ):
        """
        Place a mod using NCA logic within a scope.
        
        Args:
            mod: The mod to place
            available: Currently available params
            scope_start: Starting node for this scope (None = from start)
        """
        scope_start = scope_start or self.start_node
        
        # Find nodes needing this mod
        needing_nodes = self.get_nodes_needing_mod(mod, available)
        
        if not needing_nodes:
            print(f"  No nodes need {mod['name']} (params already available)")
            return
        
        print(f"  Nodes needing {mod['name']}: {needing_nodes}")
        
        # Filter to nodes reachable from scope_start
        reachable = nx.descendants(self.graph, scope_start) | {scope_start}
        needing_in_scope = needing_nodes & reachable
        
        if not needing_in_scope:
            print(f"  No nodes in scope need {mod['name']}")
            return
        
        # Find NCA of needing nodes
        nca = self.find_nca(needing_in_scope)
        print(f"  NCA of needing nodes: {nca}")
        
        # Check if NCA is a gateway with multiple outgoing paths
        nca_type = self.graph.nodes[nca].get('type', '')
        out_degree = self.graph.out_degree(nca)
        
        # Special case: NCA is the start event
        # Nothing can come before start, so place after start, before first successor
        if nca_type == 'startEvent':
            successors = list(self.graph.successors(nca))
            if successors:
                # If first successor is a gateway, handle it
                first_succ = successors[0]
                first_succ_type = self.graph.nodes[first_succ].get('type', '')
                first_succ_out = self.graph.out_degree(first_succ)
                
                if first_succ_type == 'exclusiveGateway' and first_succ_out > 1:
                    self._handle_gateway_nca(first_succ, mod, available, needing_in_scope)
                else:
                    # Place mod after start, before first successor
                    self._add_placement(mod, first_succ, needing_in_scope, available)
            return
        
        if nca_type == 'exclusiveGateway' and out_degree > 1:
            # This is a split gateway - check each branch
            self._handle_gateway_nca(nca, mod, available, needing_in_scope)
        else:
            # Not a gateway - place mod here
            self._add_placement(mod, nca, needing_in_scope, available)
    
    def _handle_gateway_nca(
        self, 
        gateway: str, 
        mod: Dict, 
        available: Set[str],
        needing_nodes: Set[str]
    ):
        """Handle NCA that is a gateway - check if all branches need mod."""
        
        successors = list(self.graph.successors(gateway))
        print(f"  Gateway {gateway} has {len(successors)} branches")
        
        # For each branch, check if any needing node is reachable
        branches_needing = []
        
        for succ in successors:
            # Get all nodes reachable from this branch
            branch_reachable = nx.descendants(self.graph, succ) | {succ}
            
            # Check if any needing node is in this branch
            if needing_nodes & branch_reachable:
                branches_needing.append(succ)
        
        print(f"  Branches needing mod: {branches_needing} (out of {len(successors)})")
        
        if len(branches_needing) == len(successors):
            # ALL branches need this mod → place ONCE before gateway
            print(f"  → All branches need mod, placing before gateway")
            self._add_placement(mod, gateway, needing_nodes, available, before_gateway=True)
        else:
            # Only SOME branches need mod → recursively place on each
            print(f"  → Only some branches need mod, placing on each")
            for branch_start in branches_needing:
                branch_reachable = nx.descendants(self.graph, branch_start) | {branch_start}
                branch_needing = needing_nodes & branch_reachable
                
                # Recursive call for this branch
                self._place_mod_in_branch(mod, gateway, branch_start, branch_needing, available)
    
    def _place_mod_in_branch(
        self,
        mod: Dict,
        gateway: str,
        branch_start: str,
        needing_nodes: Set[str],
        available: Set[str]
    ):
        """Place mod within a specific branch."""
        
        # Check if branch_start is another gateway
        branch_type = self.graph.nodes[branch_start].get('type', '')
        out_degree = self.graph.out_degree(branch_start)
        
        if branch_type == 'exclusiveGateway' and out_degree > 1:
            # Nested gateway - find NCA within this nested structure
            nca = self.find_nca(needing_nodes)
            if nca == branch_start:
                # NCA is this nested gateway - recurse
                self._handle_gateway_nca(branch_start, mod, available, needing_nodes)
            else:
                # Place at NCA (before the nested gateway)
                self._add_placement(mod, branch_start, needing_nodes, available, 
                                   after_node=gateway)
        else:
            # Regular node - place mod here
            self._add_placement(mod, branch_start, needing_nodes, available, 
                               after_node=gateway)
    
    def _add_placement(
        self,
        mod: Dict,
        target: str,
        needing_nodes: Set[str],
        available: Set[str],
        before_gateway: bool = False,
        after_node: Optional[str] = None
    ):
        """Add a mod placement to the results with proper chaining."""
        
        mod_id = generate_task_id()
        
        # Determine the "before" node (what comes after this mod)
        before = target
        
        # IMPORTANT: Nothing can come BEFORE the start event
        # If target is the start event, we need to place after start, before its successor
        if target == self.start_node:
            successors = list(self.graph.successors(self.start_node))
            if successors:
                before = successors[0]
                target = successors[0]  # Update target for tracking
        
        # Determine the "after" node (what comes before this mod)
        # Check if there's already a mod placed going to this target
        if target in self.last_mod_before:
            # Chain after the previous mod
            prev_mod_id = self.last_mod_before[target]
            after = prev_mod_id
            
            # IMPORTANT: Update the previous mod's 'before' to point to THIS mod
            for p in self.placements:
                if p['modId'] == prev_mod_id:
                    p['before'] = mod_id
                    break
        elif before_gateway:
            # Place before a gateway - use gateway's predecessor
            preds = list(self.graph.predecessors(before))
            after = preds[0] if preds else self.start_node
            # If predecessor is start event, that's fine - we place after start
        elif after_node:
            # Explicit after node provided
            after = after_node
        else:
            # Default: use target's predecessor
            preds = list(self.graph.predecessors(before))
            after = preds[0] if preds else self.start_node
        
        placement = {
            'modId': mod_id,
            'modName': mod['name'],
            'after': after,
            'before': before,
            'needing_nodes': list(needing_nodes),
            'reason': f'NCA placement for nodes needing {mod["name"]}'
        }
        
        self.placements.append(placement)
        
        # Update tracking: this mod is now the last mod before the original target
        self.last_mod_before[target] = mod_id
        
        print(f"  → PLACED: {mod['name']} after {after}, before {before}")


# =============================================================================
# MAIN - Test with user's BPMN
# =============================================================================

def main():
    from pathlib import Path
    import json
    
    print("=" * 70)
    print("NCA-BASED BPMN MOD OPTIMIZER")
    print("=" * 70)
    
    random.seed(42)
    
    # Use the user's BPMN
    bpmn_path = Path(__file__).parent.parent / "tests" / "fixtures" / "user_process_camunda.bpmn"
    
    optimizer = NCAOptimizer()
    graph = optimizer.parse_bpmn(bpmn_path)
    
    print("\n[BPMN STRUCTURE]")
    print(f"Nodes: {len(graph.nodes())}")
    print(f"Start: {optimizer.start_node}")
    print(f"End nodes: {optimizer.end_nodes}")
    
    print("\n[TASKS AND INPUTS]")
    for node_id in graph.nodes():
        node = graph.nodes[node_id]
        if node.get('type') in ('serviceTask', 'task'):
            print(f"  {node_id}: '{node.get('name')}' needs {node.get('inputs', set())}")
    
    # Mods definition
    mods = [
        {
            "name": "GetAccountDetails",
            "inputs": ["accountNumber"],
            "outputs": ["emailId", "accountId", "customerId"]
        },
        {
            "name": "SendEmail",
            "inputs": ["emailId"],
            "outputs": ["emailStatus"]
        }
    ]
    
    available_params = ["accountNumber"]
    
    print("\n[MODS]")
    for mod in mods:
        print(f"  {mod['name']}: {mod['inputs']} -> {mod['outputs']}")
    
    # Compute placements
    placements = optimizer.compute_placements(mods, available_params)
    
    print("\n" + "=" * 70)
    print("FINAL PLACEMENTS")
    print("=" * 70)
    
    for p in placements:
        print(f"\n[{p['modId']}] {p['modName']}")
        print(f"  After:  {p['after']}")
        print(f"  Before: {p['before']}")
    
    print("\n" + "=" * 70)
    print("JSON OUTPUT")
    print("=" * 70)
    
    result = {
        "placements": [
            {
                "modId": p["modId"],
                "modName": p["modName"],
                "after": p["after"],
                "before": p["before"]
            }
            for p in placements
        ]
    }
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
