import os
import json
from datetime import datetime
import random

import pm4py
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.log import Trace, Event
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.objects.petri.petrinet import PetriNet, Marking
from pm4py.objects.petri.utils import petri_utils

# -----------------------------
# CONFIGURATION
# -----------------------------
OUTPUT_FOLDER = "cortado_snapshots"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# ==========================================================
# 1. SYNTHETIC DATA GENERATION
# ==========================================================
def generate_synthetic_xes(output_file: str, num_traces=10, seed=42):
    """
    Generates a small synthetic XES log for demonstration.
    Each trace is a random sequence of events from a small set of activities.
    The result is saved to 'output_file'.
    """
    random.seed(seed)

    # Activities pool
    activities = ["A", "B", "C", "D", "E", "F", "G"]

    # Create an empty PM4Py log object
    from pm4py.objects.log.log import EventLog
    synthetic_log = EventLog()

    for i in range(num_traces):
        trace = Trace()
        # We'll create a random length between 3 and 8
        trace_length = random.randint(3, 8)
        for j in range(trace_length):
            event = Event()
            # Randomly select an activity
            event["concept:name"] = random.choice(activities)
            # Optionally put a timestamp or other attributes
            event["time:timestamp"] = datetime.now()
            trace.append(event)
        synthetic_log.append(trace)

    # Export to XES
    xes_exporter.apply(synthetic_log, output_file)
    print(f"[INFO] Synthetic log generated -> {output_file} (Traces: {num_traces})")


# ==========================================================
# 2. LOAD / PRE-PROCESS LOG
# ==========================================================
def load_xes_log(file_path):
    """
    Uses PM4Py to read the given XES file and returns a log object.
    """
    print(f"[INFO] Loading XES log from '{file_path}'...")
    log = xes_importer.apply(file_path)
    print("[INFO] Log loaded successfully.")
    return log


def ignore_selected_activities(log, ignored_activities):
    """
    Removes events whose 'concept:name' is in the ignored_activities set.
    This modifies the log in place.
    """
    if not ignored_activities:
        return log

    print(f"[INFO] Ignoring these activities: {ignored_activities}")
    for trace in log:
        events_to_remove = []
        for event in trace:
            if event["concept:name"] in ignored_activities:
                events_to_remove.append(event)
        for e in events_to_remove:
            trace.remove(e)
    return log


def filter_variants_by_start_end(log, start_activities=None, end_activities=None):
    """
    Filters out traces that do NOT start with any activity in start_activities
    or do NOT end with any activity in end_activities.
    """
    if not start_activities and not end_activities:
        return log

    filtered_log = pm4py.objects.log.log.EventLog()
    print("[INFO] Filtering variants by start/end activities...")

    for trace in log:
        if len(trace) == 0:
            continue

        first_activity = trace[0]["concept:name"]
        last_activity = trace[-1]["concept:name"]

        start_ok = (not start_activities) or (first_activity in start_activities)
        end_ok = (not end_activities) or (last_activity in end_activities)

        if start_ok and end_ok:
            filtered_log.append(trace)

    print(f"[INFO] {len(filtered_log)} / {len(log)} traces remain after start/end filter.")
    return filtered_log


# ==========================================================
# 3. VARIANT ANALYSIS
# ==========================================================
def extract_variants(log):
    """
    Extracts distinct variants (activity sequences) from the PM4Py log.
    Returns a dict: { variant_name : [list_of_traces], ... }
    """
    variants_dict = variants_filter.get_variants(log)
    print(f"[INFO] Found {len(variants_dict)} distinct variants in the (filtered) log.")
    return variants_dict


# Cortado-like "merge parallel & cycle" logic
def reduce_variants_cortado_style(variants_dict):
    """
    Demonstrates a naive approach to merging concurrency and cycles within variants.
    Returns a dict of { new_variant_name: activity_list }.
    We only keep ONE representative sequence per variant_name.
    """
    reduced_variants = {}
    for var_name, traces in variants_dict.items():
        if not traces:
            continue
        # We only analyze the first trace (since they share the same variant)
        act_seq = [evt["concept:name"] for evt in traces[0]]

        # Merge concurrency
        act_seq = merge_parallel_tasks(act_seq)
        # Merge cycles
        act_seq = merge_cyclic_tasks(act_seq)

        # Create a new "variant name"
        new_variant_name = f"[{'->'.join(act_seq)}]"
        reduced_variants[new_variant_name] = act_seq

    print(f"[INFO] Reduced to {len(reduced_variants)} variant(s) after concurrency/cycle merging.")
    return reduced_variants


def merge_parallel_tasks(sequence):
    """
    Naive concurrency approach:
    If two adjacent events differ, we consider them possibly concurrent
    and combine them as "A||B" if they appear next to each other.
    """
    merged_seq = []
    i = 0
    while i < len(sequence):
        if i < len(sequence) - 1 and sequence[i] != sequence[i + 1]:
            # Combine as concurrency block
            merged_seq.append(sequence[i] + "||" + sequence[i + 1])
            i += 2  # skip the next event
        else:
            merged_seq.append(sequence[i])
            i += 1
    return merged_seq


def merge_cyclic_tasks(sequence):
    """
    Naive cycle approach:
    If an activity reappears, we unify it into a single cyc block: "A*".
    """
    visited = set()
    new_seq = []
    for act in sequence:
        if act not in visited:
            new_seq.append(act)
            visited.add(act)
        else:
            # Mark cycle
            # E.g., if 'A' reappears, merge into "A*"
            # If the last in new_seq != "A*", rename it
            if new_seq and not new_seq[-1].endswith("*"):
                new_seq[-1] = new_seq[-1] + "*"
    return new_seq


# ==========================================================
# 4. PARTIAL ORDER & PETRI NET CONSTRUCTION
# ==========================================================
def build_partial_order(reduced_variants):
    """
    Builds a partial order from the reduced variant sequences.
    Output: adjacency dict { activity : set of successors }
    """
    partial_order = {}
    for seq in reduced_variants.values():
        for i, act in enumerate(seq):
            if act not in partial_order:
                partial_order[act] = set()
            if i < len(seq) - 1:
                nxt = seq[i + 1]
                partial_order[act].add(nxt)
    print(f"[INFO] Built partial order with {len(partial_order)} node(s).")
    return partial_order


def partial_order_to_petrinet(partial_order):
    """
    Converts the partial order into a naive Petri net.
    - Each unique activity => Transition
    - Insert places between transitions
    - Identify initial and final transitions for Markings
    """
    net = PetriNet("CortadoStyleNet")
    transitions_map = {}

    # Create transitions
    for act in partial_order:
        t = PetriNet.Transition(act, label=act)
        net.transitions.add(t)
        transitions_map[act] = t

    # Add places between transitions
    for act, successors in partial_order.items():
        tA = transitions_map[act]
        for succ_act in successors:
            tB = transitions_map[succ_act]
            # create place
            p = PetriNet.Place(f"p_{act}_to_{succ_act}")
            net.places.add(p)
            petri_utils.add_arc_from_to(tA, p, net)
            petri_utils.add_arc_from_to(p, tB, net)

    # Identify initial marking (no incoming arcs)
    # Identify final marking (no outgoing arcs)
    im = Marking()
    fm = Marking()

    # Build reverse lookup
    def incoming_acts(target):
        inc = []
        for a, scs in partial_order.items():
            if target in scs:
                inc.append(a)
        return inc

    for act in partial_order:
        inc = incoming_acts(act)
        if len(inc) == 0:
            # No incoming arcs => create place with 1 token
            p_init = PetriNet.Place(f"INIT_{act}")
            net.places.add(p_init)
            petri_utils.add_arc_from_to(p_init, transitions_map[act], net)
            im[p_init] = 1

        if len(partial_order[act]) == 0:
            # No outgoing => final place
            p_final = PetriNet.Place(f"END_{act}")
            net.places.add(p_final)
            petri_utils.add_arc_from_to(transitions_map[act], p_final, net)
            fm[p_final] = 1

    print("[INFO] Petri net constructed.")
    return net, im, fm


# ==========================================================
# 5. SNAPSHOT CREATION
# ==========================================================
def create_snapshot(snapshot_id, reduced_variants, partial_order, net, im, fm):
    """
    Saves the discovered Petri net, partial order, and variants in JSON format.
    """
    # For simplicity, just store net as a string representation
    net_str = str(net)

    snapshot_data = {
        "snapshot_id": snapshot_id,
        "timestamp": datetime.now().isoformat(),
        "reduced_variants": reduced_variants,
        "partial_order": {k: list(v) for k, v in partial_order.items()},
        "petri_net_representation": net_str,
        "initial_marking": {pl.name: cnt for pl, cnt in im.items()},
        "final_marking": {pl.name: cnt for pl, cnt in fm.items()}
    }

    out_path = os.path.join(OUTPUT_FOLDER, f"{snapshot_id}.json")
    with open(out_path, "w") as f:
        json.dump(snapshot_data, f, indent=4)
    print(f"[INFO] Snapshot saved -> {out_path}")
    return snapshot_data


# ==========================================================
# 6. MAIN PIPELINE
# ==========================================================
def process_log_cortado_extended(
        file_path,
        ignored_activities=None,
        start_activities=None,
        end_activities=None
):
    """
    Demonstrates a Cortado-like pipeline:
      1) Load Log
      2) Ignore selected activities
      3) Filter by start/end
      4) Extract variants
      5) Reduce them with concurrency & cycle merges
      6) Build partial order
      7) Convert to Petri net
      8) Create snapshot
    """
    # 1) Load
    log = load_xes_log(file_path)

    # 2) Ignore activities
    log = ignore_selected_activities(log, ignored_activities)

    # 3) Filter by start/end
    log = filter_variants_by_start_end(log, start_activities, end_activities)

    # 4) Extract variants
    variants_dict = extract_variants(log)

    # 5) Reduce concurrency/cycles
    reduced_variants = reduce_variants_cortado_style(variants_dict)

    # 6) Build partial order
    partial_order = build_partial_order(reduced_variants)

    # 7) Convert partial order -> Petri net
    net, im, fm = partial_order_to_petrinet(partial_order)

    # 8) Create snapshot
    snapshot_id = f"cortado_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    snapshot = create_snapshot(
        snapshot_id, reduced_variants, partial_order, net, im, fm
    )

    print("[INFO] Cortado-inspired pipeline completed.\n")
    return snapshot


# ==========================================================
# 7. DEMO USAGE
# ==========================================================
if __name__ == "__main__":
    # 1) Generate a synthetic XES file (optional step)
    synthetic_xes_path = "synthetic_log.xes"
    generate_synthetic_xes(synthetic_xes_path, num_traces=12, seed=123)

    # 2) Run the pipeline
    #    Example: ignore activity "C", filter for traces that start with "A" and end with "F"
    final_snapshot = process_log_cortado_extended(
        file_path=synthetic_xes_path,
        ignored_activities={"C"},       # or set() if you don't want to ignore
        start_activities={"A"},
        end_activities={"F"}
    )

    # Print final snapshot data
    print("[INFO] FINAL SNAPSHOT DATA:")
    print(json.dumps(final_snapshot, indent=4))
