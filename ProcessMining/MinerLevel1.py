import os
import json
from datetime import datetime

import pm4py
from pm4py.objects.log.importer.xes import importer as xes_importer

# For Petri net discovery (Alpha Miner)
from pm4py.algo.discovery.alpha import algorithm as alpha_miner

# For Token Replay-based conformance checking
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay

# For variant analysis
from pm4py.algo.filtering.log.variants import variants_filter

# Petri net objects
from pm4py.objects.petri.petrinet import PetriNet, Marking
from pm4py.objects.petri.petrinet import PetriNet as PN
from pm4py.objects.petri.utils import petri_utils

# We’ll store snapshots here
OUTPUT_FOLDER = "snapshots_rework"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

FITNESS_THRESHOLD = 0.8  # If fitness < 0.8, we attempt to repair the net


# -------------------------------------------------------------------------
# 1. LOAD LOG
# -------------------------------------------------------------------------
def load_xes_log(file_path):
    """
    Loads an XES file and returns a PM4Py log object.
    """
    print(f"[INFO] Loading XES log from '{file_path}'...")
    log = xes_importer.apply(file_path)
    print("[INFO] Log loaded successfully.")
    return log


# -------------------------------------------------------------------------
# 2. DISCOVER A PETRI NET (ALPHA MINER)
# -------------------------------------------------------------------------
def discover_process_model(log):
    """
    Discovers a Petri net model using the Alpha Miner.
    Returns (net, initial_marking, final_marking).
    """
    print("[INFO] Discovering Petri net using Alpha Miner...")
    net, im, fm = alpha_miner.apply(log)
    print("[INFO] Discovery completed.")
    return net, im, fm


# -------------------------------------------------------------------------
# 3. CONFORMANCE CHECK & FITNESS
# -------------------------------------------------------------------------
def compute_fitness(log, net, im, fm):
    """
    Performs token replay on the log/net to get replay_fitness metrics.
    Returns a dictionary with fitness info (e.g., log_fitness, percentage_of_fitted_traces).
    """
    print("[INFO] Computing replay fitness via Token Replay...")
    fitness_result = token_replay.apply(log, net, im, fm)
    # fitness_result is typically a dictionary like:
    # {
    #   'log_fitness': 0.95,
    #   'percentage_of_fitted_traces': 0.90,
    #   ...
    # }
    print(f"[INFO] Fitness result: {fitness_result}")
    return fitness_result


def perform_conformance_check(log, net, im, fm):
    """
    Performs detailed token replay conformance checking, returning
    a list of dicts (one per trace) with details about whether each trace is fit, etc.
    """
    print("[INFO] Performing conformance check with token replay...")
    token_replay_results = token_replay.apply(log, net, im, fm, variant="non_missing_toks")
    # token_replay_results is a list of trace-level conformance info, e.g.:
    # [{'trace_is_fit': True, 'enabled_transitions_in_example_of_model': [], ...}, ...]
    print(f"[INFO] Conformance check completed. #Traces analyzed: {len(token_replay_results)}.")
    return token_replay_results


# -------------------------------------------------------------------------
# 4. VARIANT ANALYSIS
# -------------------------------------------------------------------------
def analyze_variants(log):
    """
    Analyzes variants and returns a summary with variant names/frequencies.
    """
    print("[INFO] Analyzing variants...")
    variants_dict = variants_filter.get_variants(log)
    variant_analysis = []
    for variant_name, traces in variants_dict.items():
        variant_analysis.append({
            "variant_name": variant_name,
            "frequency": len(traces)
        })
    print(f"[INFO] Found {len(variants_dict)} variants.")
    return variant_analysis


# -------------------------------------------------------------------------
# 5. IDENTIFY MISSING ACTIVITIES (LOG vs. NET)
# -------------------------------------------------------------------------
def identify_missing_activities(log, net):
    """
    Looks at all activities in the log, compares them to transitions in the net,
    and returns a set of missing activities.
    """
    # Gather all unique activities in the log
    log_activities = set()
    for trace in log:
        for event in trace:
            log_activities.add(event["concept:name"])

    # Gather all transition labels in the net
    net_activities = set(t.label for t in net.transitions if t.label is not None)

    # Missing = log_activities - net_activities
    missing = log_activities - net_activities
    return missing


# -------------------------------------------------------------------------
# 6. REPAIR PETRI NET (NAIVE ADDITION OF MISSING ACTIVITIES)
# -------------------------------------------------------------------------
def repair_petri_net(net, im, fm, missing_activities):
    """
    Naive approach: For each missing activity, add a new Transition,
    plus a place_in -> Transition -> place_out structure.

    Returns (updated_net, updated_im, updated_fm).
    """
    print("[INFO] Repairing Petri net by adding missing activities...")

    for act in missing_activities:
        # Create a new transition for the missing activity
        t_new = PN.Transition(name=act, label=act)
        net.transitions.add(t_new)

        # Create input/output places for naive insertion
        p_in = PN.Place(f"p_in_{act}")
        p_out = PN.Place(f"p_out_{act}")
        net.places.add(p_in)
        net.places.add(p_out)

        # arcs
        petri_utils.add_arc_from_to(p_in, t_new, net)
        petri_utils.add_arc_from_to(t_new, p_out, net)

    print("[INFO] Missing activities added to Petri net. Repair complete.")
    return net, im, fm


# -------------------------------------------------------------------------
# 7. CREATE SNAPSHOT
# -------------------------------------------------------------------------
def create_snapshot(
        snapshot_id,
        log,
        net,
        initial_marking,
        final_marking,
        fitness,
        variant_analysis,
        conformance_results
):
    """
    Creates a snapshot containing all analysis results and saves it as JSON.
    """
    print(f"[INFO] Creating snapshot {snapshot_id}...")

    # Convert Petri net to string (for storing in JSON)
    #   pm4py.objects.petri.exporter.to_string may vary by PM4Py version.
    net_string = pm4py.objects.petri.exporter.to_string(net)

    snapshot = {
        "snapshot_id": snapshot_id,
        "timestamp": datetime.now().isoformat(),
        "model": {
            "petri_net_definition": net_string,
            "initial_marking": str(initial_marking),
            "final_marking": str(final_marking)
        },
        "analysis_results": {
            "fitness": fitness,               # e.g. {'log_fitness': 0.95, 'perc_fitted_traces': 0.9, ...}
            "variant_analysis": variant_analysis,
            "conformance_results": [
                {
                    # minimal fields to store per-trace info
                    "trace_is_fit": c.get("trace_is_fit", None),
                    "enabled_transitions_in_example_of_model": c.get("enabled_transitions_in_example_of_model", [])
                }
                for c in conformance_results
            ]
        }
    }

    # Save to disk
    out_path = os.path.join(OUTPUT_FOLDER, f"{snapshot_id}.json")
    with open(out_path, "w") as f:
        json.dump(snapshot, f, indent=4)
    print(f"[INFO] Snapshot saved -> {out_path}")

    return snapshot


# -------------------------------------------------------------------------
# 8. REWORK MODEL IF FITNESS < THRESHOLD
# -------------------------------------------------------------------------
def rework_model_if_needed(snapshot, log):
    """
    If fitness < FITNESS_THRESHOLD, we:
    - Identify missing activities
    - Repair the Petri net by adding them
    - Recompute fitness
    - Update the snapshot with the new model & new fitness

    Returns the updated snapshot.
    """
    current_fitness = snapshot["analysis_results"]["fitness"].get("log_fitness", 0)
    print(f"[INFO] Current fitness: {current_fitness} / Threshold: {FITNESS_THRESHOLD}")

    if current_fitness < FITNESS_THRESHOLD:
        print("[INFO] Fitness below threshold; Attempting to rework the net...")

        # Import the net from snapshot
        net_str = snapshot["model"]["petri_net_definition"]
        net, im, fm = pm4py.objects.petri.importer.import_petri_from_string(net_str)

        # 1) Identify missing activities
        missing_acts = identify_missing_activities(log, net)
        if missing_acts:
            print(f"[INFO] Missing activities detected: {missing_acts}")
            # 2) Repair net
            net, im, fm = repair_petri_net(net, im, fm, missing_acts)
            # 3) Recompute fitness
            new_fitness = compute_fitness(log, net, im, fm)
            # 4) Perform conformance check again
            new_conformance = perform_conformance_check(log, net, im, fm)
            # 5) Update snapshot
            net_string = pm4py.objects.petri.exporter.to_string(net)
            snapshot["model"]["petri_net_definition"] = net_string
            snapshot["analysis_results"]["fitness"] = new_fitness
            snapshot["analysis_results"]["conformance_results"] = [
                {
                    "trace_is_fit": c.get("trace_is_fit", None),
                    "enabled_transitions_in_example_of_model": c.get("enabled_transitions_in_example_of_model", [])
                }
                for c in new_conformance
            ]
            print("[INFO] Net reworked successfully. New fitness:", new_fitness)
        else:
            print("[WARN] No missing activities found, but fitness is still low. Consider advanced re-discovery.")
    else:
        print("[INFO] Fitness is acceptable; no rework needed.")

    return snapshot


# -------------------------------------------------------------------------
# 9. MAIN PIPELINE
# -------------------------------------------------------------------------
def process_log_pipeline(file_path):
    """
    End-to-end pipeline:
      1) Load log
      2) Discover Petri net
      3) Compute fitness
      4) Analyze variants
      5) Conformance check
      6) Create snapshot
      7) Rework model if needed
    """
    # Step 1: Load
    log = load_xes_log(file_path)

    # Step 2: Discover
    net, im, fm = discover_process_model(log)

    # Step 3: Compute fitness
    fitness_result = compute_fitness(log, net, im, fm)

    # Step 4: Variant analysis
    variant_result = analyze_variants(log)

    # Step 5: Conformance check
    conformance_result = perform_conformance_check(log, net, im, fm)

    # Step 6: Create snapshot
    snapshot_id = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    snapshot = create_snapshot(
        snapshot_id=snapshot_id,
        log=log,
        net=net,
        initial_marking=im,
        final_marking=fm,
        fitness=fitness_result,
        variant_analysis=variant_result,
        conformance_results=conformance_result
    )

    # Step 7: Rework model if needed
    snapshot = rework_model_if_needed(snapshot, log)

    print("\n[INFO] Pipeline complete. Final snapshot data:")
    print(json.dumps(snapshot, indent=4))
    return snapshot


# -------------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------------
if __name__ == "__main__":
    # Replace with your actual XES file path:
    XES_FILE_PATH = "example.xes"

    # Run the pipeline
    final_snapshot = process_log_pipeline(XES_FILE_PATH)
