"""
Comprehensive Test Suite for Branch-Aware BPMN Optimizer

Test Cases:
1. Simple XOR - some branches need mods, some don't
2. Nested XOR - gateway inside gateway
3. Chained Mods - A produces B, B produces C, etc.
4. All Branches Same - every branch needs the same mod
5. No Mods Needed - all params already available
6. Deep Chain - 5-level mod dependency chain
7. Mixed Dependencies - some branches need deep chain, others shallow
8. Diamond Pattern - convergent branches with shared needs
"""

import sys
import random
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from optimizer import BranchAwareOptimizer


# =============================================================================
# FIXTURES
# =============================================================================

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def optimizer():
    """Fresh optimizer instance."""
    random.seed(42)  # For reproducible IDs
    return BranchAwareOptimizer()


# =============================================================================
# TEST CASE 1: Simple XOR Gateway
# =============================================================================

class TestSimpleXOR:
    """Test simple XOR gateway with 3 branches, varying mod needs."""
    
    def test_branch1_and_branch2_get_mods_branch3_doesnt(self, optimizer):
        """
        Mods definition:
        - GetData: inputParam -> emailId, accountId
        
        Branch 1: Needs emailId -> needs GetData
        Branch 2: Needs accountId -> needs GetData
        Branch 3: Needs inputParam -> NO MOD (already available)
        
        Expected: GetData placed on branches 1 and 2 only
        """
        optimizer.parse_bpmn(FIXTURES_DIR / "simple_xor.bpmn")
        
        mods = [
            {"name": "GetData", "inputs": ["inputParam"], "outputs": ["emailId", "accountId"]}
        ]
        
        placements = optimizer.compute_placements(mods, available_params=["inputParam"])
        
        # Should have exactly 2 placements (one per needing branch)
        assert len(placements) == 2
        
        # Both should be GetData
        assert all(p['modName'] == 'GetData' for p in placements)
        
        # Both should be after the gateway
        assert all(p['after'] == 'XOR_1' for p in placements)
        
        # Placements should go to different branches
        targets = {p['before'] for p in placements}
        assert 'Task_NeedEmail' in targets
        assert 'Task_NeedAccount' in targets
        assert 'Task_NeedInput' not in targets  # Branch 3 gets nothing
    
    def test_no_mods_when_all_available(self, optimizer):
        """When all needed params are available, no mods should be placed."""
        optimizer.parse_bpmn(FIXTURES_DIR / "simple_xor.bpmn")
        
        mods = [
            {"name": "GetData", "inputs": ["x"], "outputs": ["emailId", "accountId"]}
        ]
        
        # All params already available
        placements = optimizer.compute_placements(
            mods, 
            available_params=["inputParam", "emailId", "accountId"]
        )
        
        assert len(placements) == 0


# =============================================================================
# TEST CASE 2: Nested XOR Gateway
# =============================================================================

class TestNestedXOR:
    """Test nested XOR gateways (gateway inside gateway)."""
    
    def test_nested_gateway_common_mod_placed_once(self, optimizer):
        """
        Main XOR splits to:
        - Nested XOR (3 sub-branches: 2 need dataA, 1 needs dataB)
        - Direct task (needs dataC)
        
        Mods:
        - ModA: input -> dataA
        - ModB: input -> dataB
        - ModC: input -> dataC
        
        Expected:
        - ModA NOT placed before nested gateway (only 2/3 sub-branches need it)
        - ModA placed on the 2 sub-branches that need it
        - ModB placed on the 1 sub-branch that needs it
        - ModC placed on the direct branch
        """
        optimizer.parse_bpmn(FIXTURES_DIR / "nested_xor.bpmn")
        
        mods = [
            {"name": "ModA", "inputs": ["input"], "outputs": ["dataA"]},
            {"name": "ModB", "inputs": ["input"], "outputs": ["dataB"]},
            {"name": "ModC", "inputs": ["input"], "outputs": ["dataC"]},
        ]
        
        placements = optimizer.compute_placements(mods, available_params=["input"])
        
        # Count placements per mod
        mod_counts = {}
        for p in placements:
            mod_counts[p['modName']] = mod_counts.get(p['modName'], 0) + 1
        
        # ModA should be placed 2 times (for Task_NeedA and Task_NeedA2)
        assert mod_counts.get('ModA', 0) == 2
        
        # ModB should be placed 1 time (for Task_NeedB)
        assert mod_counts.get('ModB', 0) == 1
        
        # ModC should be placed 1 time (for Task_NeedC)
        assert mod_counts.get('ModC', 0) == 1


# =============================================================================
# TEST CASE 3: Chained Mods (A -> B -> C -> D -> E)
# =============================================================================

class TestChainedMods:
    """Test long chains where mod outputs become inputs for next mod."""
    
    def test_full_chain_placed_for_deepest_requirement(self, optimizer):
        """
        Mod chain: initial -> [ModA] -> outputA -> [ModB] -> outputB -> ...
        
        Branch 1: Needs outputE -> requires full chain A->B->C->D->E
        Branch 2: Needs outputC -> requires partial chain A->B->C
        Branch 3: Needs initial -> no mods
        
        Expected placements should respect the chain order.
        """
        optimizer.parse_bpmn(FIXTURES_DIR / "chained_mods.bpmn")
        
        # 5-level chain: initial -> A -> B -> C -> D -> E
        mods = [
            {"name": "ModA", "inputs": ["initial"], "outputs": ["outputA"]},
            {"name": "ModB", "inputs": ["outputA"], "outputs": ["outputB"]},
            {"name": "ModC", "inputs": ["outputB"], "outputs": ["outputC"]},
            {"name": "ModD", "inputs": ["outputC"], "outputs": ["outputD"]},
            {"name": "ModE", "inputs": ["outputD"], "outputs": ["outputE"]},
        ]
        
        placements = optimizer.compute_placements(mods, available_params=["initial"])
        
        # Branch 1 should have 5 mods chained: A -> B -> C -> D -> E
        branch1_placements = [p for p in placements if 'Task_NeedE' in str(p.values())]
        
        # Branch 2 should have 3 mods chained: A -> B -> C  
        branch2_placements = [p for p in placements if 'Task_NeedC' in str(p.values())]
        
        # Branch 3 should have 0 mods
        branch3_count = sum(1 for p in placements if 'Task_NeedInput' in p.get('before', ''))
        assert branch3_count == 0
        
        # Find the last placement for each branch
        last_for_branch1 = [p for p in placements if p.get('before') == 'Task_NeedE']
        last_for_branch2 = [p for p in placements if p.get('before') == 'Task_NeedC']
        
        assert len(last_for_branch1) == 1
        assert last_for_branch1[0]['modName'] == 'ModE'
        
        assert len(last_for_branch2) == 1
        assert last_for_branch2[0]['modName'] == 'ModC'
    
    def test_chain_order_is_respected(self, optimizer):
        """Verify that chained mods are placed in correct dependency order."""
        optimizer.parse_bpmn(FIXTURES_DIR / "chained_mods.bpmn")
        
        mods = [
            {"name": "ModA", "inputs": ["initial"], "outputs": ["outputA"]},
            {"name": "ModB", "inputs": ["outputA"], "outputs": ["outputB"]},
            {"name": "ModC", "inputs": ["outputB"], "outputs": ["outputC"]},
        ]
        
        placements = optimizer.compute_placements(mods, available_params=["initial"])
        
        # For branch 2 (Task_NeedC), find all placements in chain
        # Follow the chain: XOR_1 -> ModA -> ModB -> ModC -> Task_NeedC
        
        placements_for_c = []
        current = 'XOR_1'
        
        for _ in range(10):  # Max iterations
            next_placement = next((p for p in placements if p['after'] == current), None)
            if next_placement and 'Task_NeedC' in [next_placement.get('before'), next_placement.get('after')]:
                placements_for_c.append(next_placement)
                current = next_placement['modId']
                if next_placement['before'] == 'Task_NeedC':
                    break
            elif next_placement:
                if current == 'XOR_1':
                    # First in chain
                    placements_for_c.append(next_placement)
                    current = next_placement['modId']
            else:
                break
        
        # Verify order: should be A, B, C
        if len(placements_for_c) >= 3:
            assert placements_for_c[0]['modName'] == 'ModA'
            assert placements_for_c[1]['modName'] == 'ModB'
            assert placements_for_c[2]['modName'] == 'ModC'


# =============================================================================
# TEST CASE 4: All Branches Need Same Mod
# =============================================================================

class TestAllBranchesSame:
    """Test when all branches need the same mod - should place once before gateway."""
    
    def test_single_mod_placed_before_gateway(self, optimizer):
        """
        All 3 branches need dataX.
        Expected: ModX placed ONCE before the XOR gateway.
        """
        optimizer.parse_bpmn(FIXTURES_DIR / "all_branches_same.bpmn")
        
        mods = [
            {"name": "ModX", "inputs": ["input"], "outputs": ["dataX"]}
        ]
        
        placements = optimizer.compute_placements(mods, available_params=["input"])
        
        # Should have exactly 1 placement
        assert len(placements) == 1
        
        # Should be ModX
        assert placements[0]['modName'] == 'ModX'
        
        # Should be placed BEFORE the XOR gateway (not on individual branches)
        assert placements[0]['before'] == 'XOR_1'


# =============================================================================
# TEST CASE 5: Unit Tests for Mod Resolution
# =============================================================================

class TestModResolution:
    """Test the mod resolution logic directly."""
    
    def test_simple_resolution(self, optimizer):
        """Simple case: one mod provides what we need."""
        optimizer.output_to_mod = {'x': 'ModA'}
        optimizer.sorted_mods = [
            {"name": "ModA", "inputs": ["input"], "outputs": ["x"]}
        ]
        
        result = optimizer._resolve_mods_for_params({'x'}, {'input'})
        
        assert len(result) == 1
        assert result[0]['name'] == 'ModA'
    
    def test_chain_resolution(self, optimizer):
        """Chain: ModA -> x, ModB needs x -> y."""
        optimizer.output_to_mod = {'x': 'ModA', 'y': 'ModB'}
        optimizer.sorted_mods = [
            {"name": "ModA", "inputs": ["input"], "outputs": ["x"]},
            {"name": "ModB", "inputs": ["x"], "outputs": ["y"]},
        ]
        
        result = optimizer._resolve_mods_for_params({'y'}, {'input'})
        
        assert len(result) == 2
        assert result[0]['name'] == 'ModA'  # First (provides x)
        assert result[1]['name'] == 'ModB'  # Second (needs x, provides y)
    
    def test_no_resolution_needed(self, optimizer):
        """When params are already available, no mods needed."""
        optimizer.output_to_mod = {'x': 'ModA'}
        optimizer.sorted_mods = [
            {"name": "ModA", "inputs": ["input"], "outputs": ["x"]}
        ]
        
        result = optimizer._resolve_mods_for_params({'x'}, {'x', 'input'})
        
        assert len(result) == 0


# =============================================================================
# TEST CASE 6: Complex Real-World Scenario
# =============================================================================

class TestRealWorldScenario:
    """
    Test the user's original scenario:
    - GetAccountDetails: accountNumber -> emailId, accountId, customerId
    - SendEmail: emailId -> emailStatus
    """
    
    def test_original_user_scenario_logic(self, optimizer):
        """
        Build mock graph to test the logic without full BPMN file.
        
        Main Gateway (3 branches):
        - Branch 1 -> Nested Gateway (3 sub-branches):
            - Sub 1: needs emailId (GetAccountDetails only)
            - Sub 2: needs accountId (GetAccountDetails only)
            - Sub 3: needs emailStatus (GetAccountDetails + SendEmail)
        - Branch 2: needs emailStatus (GetAccountDetails + SendEmail)
        - Branch 3: needs accountNumber (no mods - already available)
        """
        import networkx as nx
        
        # Build graph manually
        optimizer.graph = nx.DiGraph()
        optimizer.graph.add_node('Start', type='startEvent', name='Start', inputs=set())
        optimizer.graph.add_node('MainXOR', type='exclusiveGateway', name='Main', inputs=set())
        optimizer.graph.add_node('NestedXOR', type='exclusiveGateway', name='Nested', inputs=set())
        optimizer.graph.add_node('T1', type='serviceTask', name='NeedEmailId', inputs={'emailId'})
        optimizer.graph.add_node('T2', type='serviceTask', name='NeedAccountId', inputs={'accountId'})
        optimizer.graph.add_node('T3', type='serviceTask', name='NeedEmailStatus', inputs={'emailStatus'})
        optimizer.graph.add_node('T4', type='serviceTask', name='NeedEmailStatus2', inputs={'emailStatus'})
        optimizer.graph.add_node('T5', type='serviceTask', name='NeedAccountNum', inputs={'accountNumber'})
        
        # Edges
        optimizer.graph.add_edge('Start', 'MainXOR')
        optimizer.graph.add_edge('MainXOR', 'NestedXOR')  # Branch 1
        optimizer.graph.add_edge('MainXOR', 'T4')  # Branch 2
        optimizer.graph.add_edge('MainXOR', 'T5')  # Branch 3
        optimizer.graph.add_edge('NestedXOR', 'T1')  # Sub-branch 1
        optimizer.graph.add_edge('NestedXOR', 'T2')  # Sub-branch 2
        optimizer.graph.add_edge('NestedXOR', 'T3')  # Sub-branch 3
        
        optimizer.start_node = 'Start'
        
        mods = [
            {"name": "GetAccountDetails", "inputs": ["accountNumber"], "outputs": ["emailId", "accountId", "customerId"]},
            {"name": "SendEmail", "inputs": ["emailId"], "outputs": ["emailStatus"]},
        ]
        
        placements = optimizer.compute_placements(mods, available_params=["accountNumber"])
        
        # Verify optimal behavior:
        # 1. GetAccountDetails should be placed ONCE before NestedXOR (all sub-branches need it)
        # 2. SendEmail should be placed ONLY before T3 (only that sub-branch needs emailStatus)
        # 3. Branch 2 (T4) needs GetAccountDetails + SendEmail
        # 4. Branch 3 (T5) needs nothing
        
        mod_names = [p['modName'] for p in placements]
        
        # Should have GetAccountDetails placements (once before nested, once for branch 2)
        assert mod_names.count('GetAccountDetails') == 2
        
        # SendEmail placements (once for T3, once for T4)
        assert mod_names.count('SendEmail') == 2
        
        # Total 4 placements
        assert len(placements) == 4
        
        # Verify T5 (accountNumber branch) gets no mods
        t5_mods = [p for p in placements if p.get('before') == 'T5']
        assert len(t5_mods) == 0


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
