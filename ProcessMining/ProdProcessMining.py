import os
import json
import time
from datetime import datetime
from pm4py.visualization.petri_net.util import performance_map
from pm4py.objects.petri_net.exporter.exporter import pnml

from collections import defaultdict, Counter, deque
import itertools
import pandas as pd
import random
from datetime import datetime, timedelta
import pm4py
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.petri_net.utils import petri_utils
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments_algo
from random import choice, randint, seed
import logging
from pm4py.objects.log.util import dataframe_utils
from pm4py.analysis import check_soundness
# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
OUTPUT_FOLDER = "cortado_enhanced_snapshots"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

FITNESS_THRESHOLD = 0.8          # If alignment fitness < this, we attempt to repair
VARIANT_FREQUENCY_THRESHOLD = 1  # Min number of traces for a variant to be considered
CYCLE_DETECTION = False           # Enable merging repeated activities as loops
CONCURRENCY_DETECTION = True     # Enable concurrency detection
MERGE_STRATEGY = "bfs"           # "bfs" or "dfs" or other merging approach
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("ProcessIntelligence")


# ====================================================
# 1. SYNTHETIC EVENT DATA GENERATION
# ====================================================
# Helper function to generate random timestamps
def random_timestamp(base_time, max_duration=10):
    # time.sleep(1)
    return datetime.now()

# Define process instances with parallel, cyclic, and asynchronous activities
def generate_event_log():
    # ... (Existing event log generation code)
    data = []
    parallel_activities = [f"Parallel_{i}" for i in range(1, 10)]  # 8-9 parallel activities

    for i in range(1, 50):  # 10 process instances
        process_id = f"case_{i}"
        start_time = datetime.now()
        current_time = start_time

        # Sequential activity
        data.append([process_id, "Start", current_time, "start"])
        current_time = random_timestamp(current_time)
        time.sleep(0.1)
        # Randomize the order of parallel activities
        randomized_parallel_activities = random.sample(parallel_activities, len(parallel_activities))
        for activity in randomized_parallel_activities:
            data.append([process_id, activity, current_time, "parallel"])
        current_time = random_timestamp(current_time)
        time.sleep(0.1)
        # Cyclic activity
        for j in range(random.randint(1, 3)):  # Add a cycle
            data.append([process_id, f"Cyclic", random_timestamp(current_time), "cyclic"])
        current_time = random_timestamp(current_time)

        # Asynchronous activity
        # async_call_time = random_timestamp(current_time)
        # data.append([process_id, "Async_Call_Start", async_call_time, "async"])
        # data.append([process_id, "Async_Call_End", random_timestamp(async_call_time, 15), "async"])
        time.sleep(0.1)

        # Error boundary (optional activity)
        if random.random() > 0.7:  # 30% chance of error
            data.append([process_id, "Error_Handler", random_timestamp(current_time), "error"])
        time.sleep(0.1)

        # End activity
        data.append([process_id, "End", random_timestamp(current_time), "end"])

    return pd.DataFrame(data, columns=["case_id", "activity", "timestamp", "type"])

# Generate the synthetic event log

# ====================================================
# 2. LOAD / REMOVE SELECTED ACTIVITIES
# ====================================================

# --------------------------------------------------------------------------
# STEP 1: LOAD EVENT LOG
# --------------------------------------------------------------------------
def load_xes_log(file_path):
    """
    Loads a XES file into a PM4Py EventLog object.

    """
    # Convert to XES and PM4Py Event Log
    event_log = generate_event_log()

    event_log = dataframe_utils.convert_timestamp_columns_in_df(event_log)
    event_log = event_log.sort_values("timestamp")
    event_log = event_log.rename(columns={"case_id": "case:concept:name", "activity": "concept:name", "timestamp": "time:timestamp"})
    # event_log = event_log[event_log['concept:name'].isin([3, 6])]
    log = log_converter.apply(event_log)
    xes_exporter.apply(log, 'output.xes')
    log_converter.apply(log, "synthetic_event_log.xes")
    print(f"[INFO] Loading XES from '{file_path}'...")
    # log = xes_importer.apply(file_path)
    print("[INFO] Log loaded. Traces in log:", len(log))
    return log


# --------------------------------------------------------------------------
# STEP 2: FILTER LOW-FREQUENCY VARIANTS
# --------------------------------------------------------------------------
def filter_low_frequency_variants(log):
    """
    Removes variants that occur fewer than VARIANT_FREQUENCY_THRESHOLD times.
    This helps reduce noise for large logs.
    """
    variants_dict = variants_filter.get_variants(log)
    filtered_log = []
    dropped_count = 0
    for variant_name, traces in variants_dict.items():
        if len(traces) >= VARIANT_FREQUENCY_THRESHOLD:
            for t in traces:
                filtered_log.append(t)
        else:
            dropped_count += len(traces)

    print(f"[INFO] Removed {dropped_count} trace(s) (low-frequency variants). Remaining:", len(filtered_log))
    return filtered_log


# --------------------------------------------------------------------------
# STEP 3: EXTRACT VARIANTS (AFTER FILTER)
# --------------------------------------------------------------------------
def extract_variants(log):
    """
    Returns a dict: { variant_name : [trace1, trace2, ...], ... }.
    """
    variants_dict = variants_filter.get_variants(log)
    print(f"[INFO] #Variants after frequency filter: {len(variants_dict)}")
    return variants_dict


# --------------------------------------------------------------------------
# STEP 4: BUILD PARTIAL ORDER (FOOTPRINTS + HEURISTICS)
# --------------------------------------------------------------------------
def build_variant_partial_order(trace):
    """
    Builds a partial order for a single trace using basic footprints and
    concurrency heuristics. Extended logic includes:
      - Directly linked events get a precedence edge.
      - If we detect potential concurrency (events not in direct-follows),
        we do not enforce an order.
      - If CYCLE_DETECTION is enabled, repeated activities can form loops.
    Returns: (po, node_labels)
      where po is adjacency dict { nodeID: set_of_successors }
            node_labels is { nodeID: activity_name }
    """
    po = {}
    node_labels = {}

    # 1) Create a node for each event
    for i, evt in enumerate(trace):
        node_id = f"n_{i}"
        node_labels[node_id] = evt["concept:name"]
        po[node_id] = set()

    # 2) Direct follows: for i in [0..len-2], n_i -> n_{i+1}
    for i in range(len(trace) - 1):
        curr_id = f"n_{i}"
        next_id = f"n_{i+1}"
        po[curr_id].add(next_id)

    # 3) (Optional) cycle detection: If the same activity reappears, unify it as a loop
    if CYCLE_DETECTION:
        po, node_labels = unify_loops_in_trace(po, node_labels)

    # 4) (Optional) concurrency detection is typically done across variants.
    # For now, we keep partial orders strictly sequential for each single trace.

    return po, node_labels


def unify_loops_in_trace(po, labels):
    """
    Very naive loop detection:
      If the same activity label appears multiple times, we unify the repeated
      nodes as a single node with a self-loop or skip-structure. This is simplistic
      and can lead to merging events that had distinct contexts.
    """
    activity_to_nodes = defaultdict(list)
    for n, lab in labels.items():
        activity_to_nodes[lab].append(n)

    # For each activity with multiple nodes, unify them
    # We'll keep the first node, remove the rest, and create a loop or skip arc.
    new_po = dict(po)
    new_labels = dict(labels)
    for act, node_list in activity_to_nodes.items():
        if len(node_list) <= 1:
            continue
        node_list = sorted(node_list, key=lambda x: int(x.split("_")[1]))
        first_node = node_list[0]

        # Merge subsequent nodes into the first one
        for redundant_node in node_list[1:]:
            # connect everything that preceded redundant_node to first_node
            for pred in find_predecessors(po, redundant_node):
                new_po[pred].discard(redundant_node)
                new_po[pred].add(first_node)
            # connect first_node to everything that redundant_node pointed to
            for succ in po[redundant_node]:
                new_po[first_node].add(succ)
            # remove the redundant_node from the graph
            del new_po[redundant_node]
            del new_labels[redundant_node]

    return new_po, new_labels


def find_predecessors(po, node):
    """
    Returns all nodes that have 'node' as a successor in 'po'.
    """
    preds = []
    for n, succs in po.items():
        if node in succs:
            preds.append(n)
    return preds


# --------------------------------------------------------------------------
# STEP 5: MERGE PARTIAL ORDERS ACROSS VARIANTS (CONCURRENCY DETECTION)
# --------------------------------------------------------------------------
def merge_all_variants(variant_pos):
    """
    variant_pos is a list of (po, labels). We'll unify them into
    a single partial order with concurrency detection:
      - If two events have no ordering conflict across variants, they can be concurrent.
      - If there's a direct-follows in one variant but an inverse-follows in another, treat them as concurrent.
    We'll do a BFS-based merging to unify nodes with the same label, then reconcile edges.

    Returns: merged_po, merged_labels
    """
    # Step A: Collect all partial orders & labels
    # We unify them iteratively:
    if not variant_pos:
        return {}, {}

    # Start with the partial order of the first variant
    merged_po, merged_labels = variant_pos[0]
    next_id = 0
    # We'll rename all nodes in merged_po with global IDs to avoid collisions
    merged_po, merged_labels = rename_nodes(merged_po, merged_labels, prefix=f"g_{next_id}_")
    next_id += 1

    # Merge each subsequent partial order
    for i, (po_i, labels_i) in enumerate(variant_pos[1:], start=1):
        # rename with new prefix
        po_i, labels_i = rename_nodes(po_i, labels_i, prefix=f"g_{next_id}_")
        next_id += 1
        # unify
        merged_po, merged_labels = merge_two_partial_orders(
            merged_po, merged_labels,
            po_i, labels_i
        )

    # Next, concurrency detection: if there's a conflict in ordering, we may remove or weaken edges.
    if CONCURRENCY_DETECTION:
        apply_concurrency_detection(merged_po, merged_labels)

    return merged_po, merged_labels


def rename_nodes(po, labels, prefix="g_"):
    """
    Renames each node <n> to <prefix>_<n> to avoid collisions in global merges.
    """
    new_po = {}
    new_labels = {}
    mapping = {}
    for old_n in po:
        new_n = prefix + old_n
        mapping[old_n] = new_n
    for old_n, succs in po.items():
        new_n = mapping[old_n]
        new_po[new_n] = set(mapping[s] for s in succs)
    for old_n, lab in labels.items():
        new_n = mapping[old_n]
        new_labels[new_n] = lab
    return new_po, new_labels


def merge_two_partial_orders(poA, labelsA, poB, labelsB):
    """
    Merges partial orders A and B.
    Step 1: unify nodes with the same label if possible.
    Step 2: unify edges (if there's a direct-follows in A but not in B, keep it if no conflict).
    Step 3: handle any conflicting edges with concurrency or removal if cycle is introduced.
    """
    merged_po = dict(poA)
    merged_labels = dict(labelsA)

    # Step A: Attempt to unify nodes that have the same label
    # We'll create a label->list_of_nodes map for each PO
    label_to_nodesA = defaultdict(list)
    for n, lab in labelsA.items():
        label_to_nodesA[lab].append(n)
    label_to_nodesB = defaultdict(list)
    for n, lab in labelsB.items():
        label_to_nodesB[lab].append(n)

    # We'll unify the first pair in each label group
    local_mapBtoA = {}  # which node in B merges to which node in A
    for lab, b_nodes in label_to_nodesB.items():
        if lab in label_to_nodesA and label_to_nodesA[lab]:
            a_node = label_to_nodesA[lab][0]  # pick the first node from A
            for b_node in b_nodes:
                # unify b_node into a_node
                local_mapBtoA[b_node] = a_node

    # Step B: For each node in B that isn't unified, we add it to merged_po
    for b_node in poB:
        if b_node not in local_mapBtoA:
            merged_po[b_node] = set(poB[b_node])
            merged_labels[b_node] = labelsB[b_node]

    # Step C: unify edges for merged nodes
    for b_node, successors in poB.items():
        if b_node in local_mapBtoA:
            a_node = local_mapBtoA[b_node]
            # merge successors
            for s in successors:
                if s in local_mapBtoA:
                    # unify with mapped node
                    s_a = local_mapBtoA[s]
                    merged_po[a_node].add(s_a)
                else:
                    merged_po[a_node].add(s)
        else:
            # it's a new node in merged_po, unify successors
            for s in list(successors):
                if s in local_mapBtoA:
                    s_a = local_mapBtoA[s]
                    merged_po[b_node].add(s_a)
                    merged_po[b_node].discard(s)

    # Remove edges that cause cycles if we want concurrency
    remove_cycle_edges(merged_po)
    return merged_po, merged_labels


def remove_cycle_edges(po):
    """
    For each edge, if it creates a cycle, remove it.
    We'll do a BFS/DFS approach to detect cycles quickly.
    """
    edges_to_remove = []
    for src in list(po.keys()):
        for tgt in list(po[src]):
            if creates_cycle(po, src, tgt):
                edges_to_remove.append((src, tgt))
    for (s, t) in edges_to_remove:
        po[s].remove(t)


def creates_cycle(po, src, tgt):
    """
    Check if there's a path from tgt back to src in 'po'.
    If yes, adding edge src->tgt introduces a cycle.
    """
    visited = set()
    queue = deque([tgt])
    while queue:
        curr = queue.popleft()
        if curr == src:
            return True
        for s in po[curr]:
            if s not in visited:
                visited.add(s)
                queue.append(s)
    return False


def apply_concurrency_detection(po, labels):
    """
    If there's an ordering conflict across the merged partial orders
    (i.e., A->B in one variant, B->A in another), we remove direct edges
    to treat A and B as concurrent.
    For demonstration, we consider pairs of nodes that appear in reversed
    order in the partial order -> remove both edges.
    """
    # We'll gather adjacency for reversed order detection
    # If a->b in the adjacency, it means a < b. If also b->a, we have conflict -> remove both.
    # A more advanced approach uses footprints or concurrency footprints across variants.
    conflicting_pairs = []
    all_nodes = list(po.keys())
    for a, b in itertools.combinations(all_nodes, 2):
        # check if a->b and b->a
        if b in po[a] and a in po[b]:
            conflicting_pairs.append((a, b))

    for (a, b) in conflicting_pairs:
        # treat them as concurrent -> remove both edges
        po[a].discard(b)
        po[b].discard(a)


# --------------------------------------------------------------------------
# STEP 6: BUILD PETRI NET FROM MERGED PARTIAL ORDER
# --------------------------------------------------------------------------
def partial_order_to_petrinet(po, labels):
    net = PetriNet("CortadoEnhancedNet")
    transitions_map = {}

    # Create transitions
    for node_id, act_label in labels.items():
        t = PetriNet.Transition(node_id, label=act_label)
        net.transitions.add(t)
        transitions_map[node_id] = t

    # Create places for edges
    for src, successors in po.items():
        t_src = transitions_map[src]
        for tgt in successors:
            t_tgt = transitions_map[tgt]
            p = PetriNet.Place(f"p_{src}_to_{tgt}")
            net.places.add(p)
            petri_utils.add_arc_from_to(t_src, p, net)
            petri_utils.add_arc_from_to(p, t_tgt, net)

    # Identify initial marking / final marking
    im = Marking()
    fm = Marking()
    # A node with no incoming edges -> initial
    # A node with no outgoing edges -> final
    all_nodes = set(po.keys())
    successors_union = set()
    for s, scs in po.items():
        successors_union.update(scs)

    no_incoming = all_nodes - successors_union
    no_outgoing = [n for n, scs in po.items() if len(scs) == 0]

    for ni in no_incoming:
        p_init = PetriNet.Place(f"init_{ni}")
        net.places.add(p_init)
        petri_utils.add_arc_from_to(p_init, transitions_map[ni], net)
        im[p_init] = 1
    for no in no_outgoing:
        p_final = PetriNet.Place(f"final_{no}")
        net.places.add(p_final)
        petri_utils.add_arc_from_to(transitions_map[no], p_final, net)
        fm[p_final] = 1

    return net, im, fm


# --------------------------------------------------------------------------
# STEP 7: ALIGNMENT-BASED CONFORMANCE CHECK
# --------------------------------------------------------------------------
def alignment_conformance_check(log, net, im, fm):
    """
    Performs alignment-based conformance (often more accurate than token replay).
    Returns:
      fitness_dict: e.g. {
        'log_fitness': 0.95,
        'average_trace_fitness': 0.90,
        ...
      }
      alignment_results: list of dicts per trace alignment
    """
    # Convert log to event stream
    parameters = {
        alignments_algo.Variants.VERSION_STATE_EQUATION_A_STAR.value.Parameters.PARAM_ALIGNMENT_RESULT_IS_SYNC_PROD_AWARE: True
    }
    align_result = alignments_algo.apply_log(log, net, im, fm, parameters=parameters)

    model_soundness = check_soundness(net, im, fm, True)
    print(model_soundness)
    fitness_info = pm4py.fitness_token_based_replay(log, net, im, fm)
    alignments = pm4py.conformance_diagnostics_alignments(log, net, im, fm)
    # non_fitting = list(filter(lambda a: a['fitness']<1.0, alignments))
    # non_fitting[0]
    # alignment = non_fitting[0]['alignment']
    # trace = list(map(lambda m: m[0], filter(lambda m: m[0] !='>>', alignment)))
    # trace
    # trace = list(map(lambda m: m[1], filter(lambda m: m[1] !='>>' and m[1] is not None, alignment)))
    # trace
    # fitness_info = pm4py.fitness_alignments(log, net, im, fm)

    return fitness_info, align_result


# --------------------------------------------------------------------------
# STEP 8: IDENTIFY MISSING ACTIVITIES & REPAIR
# --------------------------------------------------------------------------
def identify_missing_activities(log, net):
    """
    Compare log activity set vs. net transitions.
    """
    log_acts = set()
    for trace in log:
        for evt in trace:
            log_acts.add(evt["concept:name"])
    net_acts = set(t.label for t in net.transitions if t.label is not None)
    return log_acts - net_acts


def repair_petri_net(net, im, fm, missing_acts):
    """
    Naive approach: For each missing act, add a Transition & in/out places.
    """
    for act in missing_acts:
        t_new = PetriNet.Transition(act, label=act)
        net.transitions.add(t_new)

        p_in = PetriNet.Place(f"p_in_{act}")
        p_out = PetriNet.Place(f"p_out_{act}")
        net.places.add(p_in)
        net.places.add(p_out)

        petri_utils.add_arc_from_to(p_in, t_new, net)
        petri_utils.add_arc_from_to(t_new, p_out, net)

    return net, im, fm


# --------------------------------------------------------------------------
# STEP 9: CREATE SNAPSHOT
# --------------------------------------------------------------------------
def create_snapshot(snapshot_id, net, im, fm, fitness_info, alignment_results,
                    merged_po, merged_labels, merged_variants_info):
    """
    Store final Petri net + analysis in JSON.
    """
    net_string = pnml.export_petri_as_string(net, im, fm)


    # Summarize alignment results minimally
    summarized_alignments = []
    for align_item in alignment_results:
        summarized_alignments.append({
            "cost": align_item.get("cost", None),
            "fitness": align_item.get("fitness", None),
            "log_moves": align_item.get("log_moves", []),
            "model_moves": align_item.get("model_moves", [])
        })

    snapshot_data = {
        "snapshot_id": snapshot_id,
        "timestamp": datetime.now().isoformat(),
        "petri_net": net_string,
        "initial_marking": str(im),
        "final_marking": str(fm),
        "fitness_info": fitness_info,
        "alignment_results": summarized_alignments,
        "merged_partial_order": {
            "adjacency": {n: list(s) for n, s in merged_po.items()},
            "labels": merged_labels
        },
        "merged_variants_info": merged_variants_info
    }

    out_path = os.path.join(OUTPUT_FOLDER, f"{snapshot_id}.json")
    with open(out_path, "w") as f:
        json.dump(snapshot_data, f, indent=4)
    print(f"[INFO] Snapshot saved to '{out_path}'")

    return snapshot_data


# --------------------------------------------------------------------------
# STEP 10: MAIN CORTADO-INSPIRED PIPELINE
# --------------------------------------------------------------------------
def cortado_enhanced_pipeline(file_path):
    """
    1) Load log
    2) Filter low-frequency variants
    3) Extract variants
    4) Build partial orders for each variant
    5) Merge partial orders (detect concurrency, unify loops)
    6) Build Petri net
    7) Do alignment-based conformance checking
    8) If fitness < threshold, identify missing activities, repair, re-check
    9) Create & store snapshot
    """
    # Step 1) Load
    log = load_xes_log(file_path)

    # Step 2) Filter out low-frequency variants
    log = filter_low_frequency_variants(log)

    # Step 3) Extract variants
    variants_dict = extract_variants(log)

    print("[INFO] Number of variants:", len(variants_dict))
    print("[INFO] Variants:", variants_dict)

    # Step 4) Build partial orders for each variant
    variant_partial_orders = []
    merged_variants_info = {}
    for variant_name, traces in variants_dict.items():
        if not traces:
            continue
        # We'll just build partial order from the first trace in each variant
        trace = traces[0]
        po, labels = build_variant_partial_order(trace)
        variant_partial_orders.append((po, labels))
        merged_variants_info[variant_name] = [evt["concept:name"] for evt in trace]

    if not variant_partial_orders:
        print("[WARN] No partial orders available after filtering. Exiting.")
        return {}

    # Step 5) Merge partial orders
    merged_po, merged_labels = merge_all_variants(variant_partial_orders)

    # Step 6) Build Petri net from merged PO
    net, im, fm = partial_order_to_petrinet(merged_po, merged_labels)

    pm4py.view_petri_net(net, im, fm, format='svg')

    # Step 7) Alignment-based conformance
    fitness_info, alignment_results = alignment_conformance_check(log, net, im, fm)
    print("[INFO] Alignment fitness:", fitness_info)

    # Step 8) If fitness < threshold, try repair
    if fitness_info["log_fitness"] < FITNESS_THRESHOLD:
        missing_acts = identify_missing_activities(log, net)
        if missing_acts:
            print("[INFO] Missing activities found. Repairing net with:", missing_acts)
            net, im, fm = repair_petri_net(net, im, fm, missing_acts)
            # Re-check conformance
            fitness_info, alignment_results = alignment_conformance_check(log, net, im, fm)
            print("[INFO] New alignment fitness after repair:", fitness_info)
        else:
            print("[WARN] Fitness below threshold but no missing activities. Consider advanced re-discovery.")

    # Step 9) Create snapshot
    snapshot_id = f"cortado_enhanced_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    snapshot_data = create_snapshot(
        snapshot_id, net, im, fm, fitness_info,
        alignment_results, merged_po, merged_labels,
        merged_variants_info
    )

    print("[INFO] Cortado-Enhanced pipeline complete.")
    return snapshot_data


# --------------------------------------------------------------------------
# DRIVER CODE
# --------------------------------------------------------------------------
if __name__ == "__main__":
    # Example usage: Provide your own XES file
    XES_FILE_PATH = "synthetic_event_log.xes"  # Replace with your real path
    final_snapshot = cortado_enhanced_pipeline(XES_FILE_PATH)
    print("\n[INFO] FINAL SNAPSHOT DATA:")
    print(json.dumps(final_snapshot, indent=4))
