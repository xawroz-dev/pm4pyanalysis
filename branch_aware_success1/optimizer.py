"""
Branch-Aware BPMN Mod Optimizer

This module provides an optimizer that places data-enrichment mods at optimal
positions in BPMN workflows, minimizing redundant mod calls.

Key concepts:
- Mod: A service that takes inputs and produces outputs (e.g., GetAccountDetails)
- Placement: Where to insert a mod in the BPMN flow (after node X, before node Y)
- Branch-aware: Only places mods on XOR gateway branches that actually need them
"""

import random
import string
from collections import defaultdict
from typing import Dict, List, Set, Any, Optional, Tuple
import networkx as nx
from lxml import etree


# =============================================================================
# CONSTANTS
# =============================================================================

BPMN_NAMESPACES = {
    'bpmn': 'http://www.omg.org/spec/BPMN/20100524/MODEL',
    'camunda': 'http://camunda.org/schema/1.0/bpmn',
}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def generate_task_id() -> str:
    """Generate a unique task ID in format Task_xxxxxxx (7 random chars)."""
    chars = string.ascii_lowercase + string.digits
    random_part = ''.join(random.choices(chars, k=7))
    return f"Task_{random_part}"


def topological_sort_mods(mods: List[Dict], output_to_mod: Dict[str, str]) -> List[Dict]:
    """
    Sort mods by dependencies using Kahn's algorithm.
    
    If Mod B needs output from Mod A, then A comes before B in the result.
    
    Args:
        mods: List of mod definitions with 'name', 'inputs', 'outputs'
        output_to_mod: Mapping from output name to producing mod name
        
    Returns:
        Topologically sorted list of mods
    """
    # Build dependency graph: mod_name -> set of mods it depends on
    dependencies = {mod['name']: set() for mod in mods}
    
    for mod in mods:
        for inp in mod.get('inputs', []):
            if inp in output_to_mod:
                producer = output_to_mod[inp]
                if producer != mod['name']:
                    dependencies[mod['name']].add(producer)
    
    # Kahn's algorithm
    in_degree = {name: len(deps) for name, deps in dependencies.items()}
    ready = [name for name, deg in in_degree.items() if deg == 0]
    sorted_names = []
    
    # Build reverse graph for updating in-degrees
    dependents = defaultdict(set)
    for mod_name, deps in dependencies.items():
        for dep in deps:
            dependents[dep].add(mod_name)
    
    while ready:
        current = ready.pop(0)
        sorted_names.append(current)
        
        for dependent in dependents[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                ready.append(dependent)
    
    # Convert names back to mod dicts
    name_to_mod = {m['name']: m for m in mods}
    return [name_to_mod[name] for name in sorted_names]


# =============================================================================
# MAIN OPTIMIZER CLASS
# =============================================================================

class BranchAwareOptimizer:
    """
    Optimizer that places mods at optimal positions in BPMN workflows.
    
    Features:
    - Analyzes XOR gateway branches independently
    - Places mods only on branches that need them
    - Handles nested gateways correctly
    - Respects mod dependency chains
    
    Example:
        optimizer = BranchAwareOptimizer()
        optimizer.parse_bpmn("process.bpmn")
        placements = optimizer.compute_placements(mods, ["initial_param"])
    """
    
    def __init__(self):
        self.graph: Optional[nx.DiGraph] = None
        self.start_node: Optional[str] = None
        self.sorted_mods: List[Dict] = []
        self.output_to_mod: Dict[str, str] = {}
        self.placements: List[Dict] = []
        
    # -------------------------------------------------------------------------
    # BPMN Parsing
    # -------------------------------------------------------------------------
    
    def parse_bpmn(self, bpmn_path: str) -> nx.DiGraph:
        """
        Parse a Camunda BPMN file and build a directed graph.
        
        Args:
            bpmn_path: Path to the BPMN XML file
            
        Returns:
            NetworkX DiGraph representing the process flow
        """
        with open(bpmn_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return self.parse_bpmn_string(content)
    
    def parse_bpmn_string(self, bpmn_xml: str) -> nx.DiGraph:
        """Parse BPMN from XML string."""
        root = etree.fromstring(bpmn_xml.encode('utf-8'))
        process = root.find('.//bpmn:process', BPMN_NAMESPACES)
        
        if process is None:
            raise ValueError("No process element found in BPMN")
        
        self.graph = nx.DiGraph()
        
        # Node types we care about
        node_types = [
            'startEvent', 'endEvent', 'serviceTask', 'task', 'userTask',
            'exclusiveGateway', 'parallelGateway', 'inclusiveGateway'
        ]
        
        # Extract nodes
        for node_type in node_types:
            for elem in process.findall(f'.//bpmn:{node_type}', BPMN_NAMESPACES):
                node_id = elem.get('id')
                name = elem.get('name', '')
                
                # Extract input parameters (what this task needs)
                inputs = self._extract_inputs(elem)
                
                self.graph.add_node(
                    node_id,
                    type=node_type,
                    name=name,
                    inputs=inputs
                )
                
                if node_type == 'startEvent':
                    self.start_node = node_id
        
        # Extract sequence flows (edges)
        for flow in process.findall('.//bpmn:sequenceFlow', BPMN_NAMESPACES):
            source = flow.get('sourceRef')
            target = flow.get('targetRef')
            if source in self.graph and target in self.graph:
                self.graph.add_edge(source, target)
        
        return self.graph
    
    def _extract_inputs(self, elem) -> Set[str]:
        """Extract camunda:inputParameter names from an element."""
        inputs = set()
        input_output = elem.find('.//camunda:inputOutput', BPMN_NAMESPACES)
        if input_output is not None:
            for param in input_output.findall('camunda:inputParameter', BPMN_NAMESPACES):
                param_name = param.get('name')
                if param_name:
                    inputs.add(param_name)
        return inputs
    
    # -------------------------------------------------------------------------
    # Main Computation
    # -------------------------------------------------------------------------
    
    def compute_placements(
        self, 
        mods: List[Dict], 
        available_params: List[str]
    ) -> List[Dict]:
        """
        Compute optimal mod placements for the parsed BPMN.
        
        Args:
            mods: List of mod definitions, each with:
                  - name: Mod identifier
                  - inputs: List of required input params
                  - outputs: List of produced output params
            available_params: Parameters already available at process start
            
        Returns:
            List of placement dictionaries with:
            - modId: Generated unique ID (Task_xxxxxxx)
            - modName: Name of the mod
            - after: Node ID that comes before this mod
            - before: Node ID that comes after this mod
        """
        if self.graph is None:
            raise ValueError("Must call parse_bpmn first")
        
        # Build output -> mod mapping
        self.output_to_mod = {}
        for mod in mods:
            for output in mod.get('outputs', []):
                self.output_to_mod[output] = mod['name']
        
        # Sort mods by dependency order
        self.sorted_mods = topological_sort_mods(mods, self.output_to_mod)
        
        # Reset placements
        self.placements = []
        
        # Start recursive processing from start node
        self._process_node(
            node_id=self.start_node,
            available=set(available_params),
            predecessor=None,
            processed=set()
        )
        
        return self.placements
    
    # -------------------------------------------------------------------------
    # Recursive Processing
    # -------------------------------------------------------------------------
    
    def _process_node(
        self,
        node_id: str,
        available: Set[str],
        predecessor: Optional[str],
        processed: Set[str]
    ):
        """Recursively process nodes, handling gateways specially."""
        
        if node_id in processed:
            return
        
        node_data = self.graph.nodes[node_id]
        node_type = node_data.get('type', '')
        
        # Is this a splitting XOR gateway?
        is_xor_split = (
            node_type == 'exclusiveGateway' and 
            self.graph.out_degree(node_id) > 1
        )
        
        if is_xor_split:
            processed.add(node_id)
            self._handle_xor_gateway(node_id, available, predecessor, processed)
        else:
            # Regular node - just continue to successors
            processed.add(node_id)
            for successor in self.graph.successors(node_id):
                self._process_node(successor, available, node_id, processed)
    
    def _handle_xor_gateway(
        self,
        gateway_id: str,
        available: Set[str],
        predecessor: Optional[str],
        processed: Set[str]
    ):
        """Handle an XOR gateway by analyzing each branch independently."""
        
        branches = list(self.graph.successors(gateway_id))
        
        # For each branch, determine what mods it needs
        branch_needs = {}  # branch_target -> list of mod names needed
        
        for branch_target in branches:
            needed_mods = self._get_mods_for_subtree(branch_target, available)
            branch_needs[branch_target] = needed_mods
        
        # Determine which mods are needed by ALL vs SOME branches
        all_needed = set()
        for mods_list in branch_needs.values():
            all_needed.update(mods_list)
        
        mods_for_all = []  # Mods needed by ALL branches
        mods_for_some = defaultdict(list)  # mod_name -> [branches needing it]
        
        for mod in self.sorted_mods:
            mod_name = mod['name']
            if mod_name not in all_needed:
                continue
            
            branches_needing = [
                b for b, needs in branch_needs.items() 
                if mod_name in needs
            ]
            
            if len(branches_needing) == len(branches):
                mods_for_all.append(mod)
            else:
                mods_for_some[mod_name] = branches_needing
        
        # Place mods needed by ALL branches BEFORE the gateway
        current_available = available.copy()
        gateway_pred = predecessor
        
        if mods_for_all:
            mod_chain = [(generate_task_id(), m['name']) for m in mods_for_all]
            
            for i, (mod_id, mod_name) in enumerate(mod_chain):
                after = gateway_pred if i == 0 else mod_chain[i-1][0]
                before = mod_chain[i+1][0] if i < len(mod_chain) - 1 else gateway_id
                
                self.placements.append({
                    'modId': mod_id,
                    'modName': mod_name,
                    'after': after,
                    'before': before,
                    'reason': f'All {len(branches)} branches need this'
                })
            
            # Update available params
            for mod in mods_for_all:
                current_available.update(mod.get('outputs', []))
        
        # Process each branch
        for branch_target in branches:
            self._process_branch(
                gateway_id=gateway_id,
                branch_target=branch_target,
                available=current_available.copy(),
                mods_for_some=mods_for_some,
                processed=processed.copy()
            )
    
    def _process_branch(
        self,
        gateway_id: str,
        branch_target: str,
        available: Set[str],
        mods_for_some: Dict[str, List[str]],
        processed: Set[str]
    ):
        """Process a single branch from a gateway."""
        
        target_type = self.graph.nodes[branch_target].get('type', '')
        
        # Is this branch target another XOR gateway?
        is_nested_xor = (
            target_type == 'exclusiveGateway' and 
            self.graph.out_degree(branch_target) > 1
        )
        
        if is_nested_xor:
            # Recursively handle nested gateway
            self._handle_xor_gateway(
                gateway_id=branch_target,
                available=available,
                predecessor=gateway_id,
                processed=processed
            )
        else:
            # Regular target - place mods needed for this specific node
            mods_needed = self._get_mods_for_node(branch_target, available)
            
            if mods_needed:
                mod_chain = [(generate_task_id(), m['name']) for m in mods_needed]
                
                for i, (mod_id, mod_name) in enumerate(mod_chain):
                    after = gateway_id if i == 0 else mod_chain[i-1][0]
                    before = mod_chain[i+1][0] if i < len(mod_chain) - 1 else branch_target
                    
                    self.placements.append({
                        'modId': mod_id,
                        'modName': mod_name,
                        'after': after,
                        'before': before,
                        'reason': f'Needed for {branch_target}'
                    })
                
                # Update available for further processing
                for mod in mods_needed:
                    available.update(mod.get('outputs', []))
            
            # Continue processing successors
            for successor in self.graph.successors(branch_target):
                self._process_node(successor, available, branch_target, processed)
    
    # -------------------------------------------------------------------------
    # Mod Resolution
    # -------------------------------------------------------------------------
    
    def _get_mods_for_node(self, node_id: str, available: Set[str]) -> List[Dict]:
        """Get mods needed for a single node's inputs."""
        node_inputs = self.graph.nodes[node_id].get('inputs', set())
        return self._resolve_mods_for_params(node_inputs, available)
    
    def _get_mods_for_subtree(self, start_node: str, available: Set[str]) -> Set[str]:
        """Get all mod names needed anywhere in a subtree."""
        needed_mods = set()
        visited = set()
        queue = [start_node]
        
        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            
            # Get mods for this node
            mods = self._get_mods_for_node(node, available)
            for mod in mods:
                needed_mods.add(mod['name'])
            
            # Add successors
            for successor in self.graph.successors(node):
                queue.append(successor)
        
        return needed_mods
    
    def _resolve_mods_for_params(
        self, 
        needed_params: Set[str], 
        available: Set[str]
    ) -> List[Dict]:
        """
        Resolve which mods are needed to satisfy parameter requirements.
        
        Handles chains: if param Z needs ModC, and ModC needs Y from ModB,
        and ModB needs X from ModA, returns [ModA, ModB, ModC].
        """
        result = []
        current_available = available.copy()
        params_to_satisfy = needed_params - current_available
        
        while params_to_satisfy:
            found = False
            
            for mod in self.sorted_mods:
                mod_name = mod['name']
                mod_outputs = set(mod.get('outputs', []))
                
                # Does this mod provide anything we need?
                if mod_outputs & params_to_satisfy:
                    # Check if mod's inputs are satisfied
                    mod_inputs = set(mod.get('inputs', []))
                    missing_inputs = mod_inputs - current_available
                    
                    # Recursively resolve missing inputs first
                    if missing_inputs:
                        prereqs = self._resolve_mods_for_params(
                            missing_inputs, current_available
                        )
                        for prereq in prereqs:
                            if prereq['name'] not in [r['name'] for r in result]:
                                result.append(prereq)
                                current_available.update(prereq.get('outputs', []))
                    
                    # Now add this mod
                    if mod_name not in [r['name'] for r in result]:
                        result.append(mod)
                        current_available.update(mod_outputs)
                    
                    params_to_satisfy -= mod_outputs
                    found = True
                    break
            
            if not found:
                break  # Can't satisfy remaining params
        
        return result
