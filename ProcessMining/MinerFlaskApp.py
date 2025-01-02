import pm4py
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.batches import algorithm as batch_miner


from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness
from pm4py.algo.evaluation.precision import algorithm as precision
from pm4py.algo.evaluation.generalization import algorithm as generalization
from pm4py.algo.evaluation.simplicity import algorithm as simplicity

from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.petri_net.utils import petri_utils
import json
import os
from datetime import datetime

# --------------- Configuration Parameters ---------------
OUTPUT_FOLDER = "snapshots"  # Directory to store snapshots
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

FITNESS_THRESHOLD = 0.8      # If fitness is below this, we attempt to repair the model


# --------------- Helper Functions ---------------
def load_xes_log(file_path):
    """
    Loads an XES file into a PM4Py log object.
    """
    print(f"Loading XES log from {file_path}...")
    log = xes_importer.apply(file_path)
    print("Log loaded successfully.")
    return log


def discover_process_model(log):
    """
    Discovers a Petri net model using Alpha Miner.
    """
    print("Discovering process model (Alpha Miner)...")
    net, im, fm = inductive_miner.apply(log)
    print("Process model discovered successfully.")
    return net, im, fm


def compute_fitness(log, net, initial_marking, final_marking):
    """
    Evaluates the fitness of the log against the Petri net model.
    """
    print("Calculating fitness...")
    fitness  = replay_fitness.apply(log, net, initial_marking, final_marking)
    print(f"Fitness result: {fitness}")
    return fitness

def compute_precision(log, net, initial_marking, final_marking):
    """
    Evaluates the precision of the log against the Petri net model.
    """
    print("Calculating precision...")
    precision_result  = precision.apply(log, net, initial_marking, final_marking)
    print(f"Precision result: {precision_result}")
    return precision_result

def compute_simplicity(net):
    """
    Evaluates the simplicity of the log against the Petri net model.
    """
    print("Calculating simplicity...")
    simplicity_result  = simplicity.apply(net)
    print(f"Simplicity result: {simplicity_result}")
    return simplicity_result

def compute_generalization(log, net, initial_marking, final_marking):
    """
    Evaluates the generalization of the log against the Petri net model.
    """
    print("Calculating generalization...")
    generalization_result  = generalization.apply(log, net, initial_marking, final_marking)
    print(f"Generalization result: {generalization_result}")
    return generalization_result


def analyze_variants(log):
    """
    Analyzes variants and their metrics from the log.
    """
    print("Analyzing variants...")
    variants_dict = variants_filter.get_variants(log)
    variant_analysis = []
    for variant_name, variant_logs in variants_dict.items():
        variant_analysis.append({
            "variant_name": variant_name,
            "frequency": len(variant_logs)
        })
    print("Variant analysis completed.")
    return variant_analysis


def perform_conformance_check(log, net, initial_marking, final_marking):
    """
    Performs conformance checking using Token Replay.
    Returns token replay results, which can help identify missing activities.
    """
    print("Performing conformance checking (Token Replay)...")
    token_replay_result = token_replay.apply(log, net, initial_marking, final_marking)
    print("Conformance checking completed.")
    return token_replay_result


def identify_missing_activities(log, net):
    """
    Identifies activities from the event log that do not exist in the Petri net transitions.
    This will help us determine which 'new' activities need to be added.
    """
    # Get set of all activities in the log
    activities_in_log = set()
    for trace in log:
        for event in trace:
            activities_in_log.add(event["concept:name"])

    # Get set of all transitions in the net
    transitions_in_net = set(t.label for t in net.transitions if t.label is not None)

    # Missing activities are those not in net transitions
    missing_activities = activities_in_log - transitions_in_net
    return missing_activities


def repair_petri_net(net, im, fm, missing_activities):
    """
    Naive approach to add missing activities to the existing Petri net model.
    - For each missing activity, create a new transition.
    - Insert minimal places/arcs to keep the net connected and maintain soundness (high-level example).
    """
    print("Repairing Petri net by adding missing activities...")

    for activity in missing_activities:
        # Create a new transition for the activity
        transition = PetriNet.Transition(activity, activity)
        net.transitions.add(transition)

        # For demonstration, create:
        # 1) A new place leading into the transition
        # 2) A new place leading out of the transition
        place_in = PetriNet.Place(f"p_in_{activity}")
        place_out = PetriNet.Place(f"p_out_{activity}")
        net.places.add(place_in)
        net.places.add(place_out)

        # Link place_in -> transition -> place_out
        petri_utils.add_arc_from_to(place_in, transition, net)
        petri_utils.add_arc_from_to(transition, place_out, net)

        # In a more sophisticated approach, you'd figure out *where* in the net
        # these arcs should be placed or how they might merge with existing places.
        # This naive approach simply appends these transitions "somewhere" in the net.

    print("Missing activities added to the Petri net. Model repair complete.")
    return net, im, fm


def create_snapshot(snapshot_id, log, net, initial_marking, final_marking,
                    fitness, variant_analysis, conformance_results):
    """
    Creates a snapshot containing all analysis results and saves it as JSON.
    """
    print(f"Creating snapshot {snapshot_id}...")

    # Convert Petri net to string (for storing in JSON)
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
                "fitness": fitness,
                "variant_analysis": variant_analysis,
                "conformance_results": [
                    {
                        "trace_is_fit": c["trace_is_fit"],
                        "enabled_transitions_in_example_of_model": c["enabled_transitions_in_example_of_model"],
                        # Add more fields from c as needed
                    } for c in conformance_results
                ]
            }
    }

    output_path = os.path.join(OUTPUT_FOLDER, f"{snapshot_id}.json")
    with open(output_path, "w") as f:
        json.dump(snapshot, f, indent=4)
    print(f"Snapshot saved at {output_path}.")
    return snapshot


def rework_model_if_needed(snapshot, log):
    """
    Reworks the process model if fitness is below a certain threshold.
    Instead of rediscovering from scratch, we attempt to 'repair' the existing Petri net
    by adding missing activities.
    """
    current_fitness = snapshot["analysis_results"]["fitness"]["log_fitness"]
    print(f"Current fitness: {current_fitness}. Threshold: {FITNESS_THRESHOLD}")
    if current_fitness < FITNESS_THRESHOLD:
        print("Fitness below threshold -> Checking for missing activities in the model...")

        # Retrieve net definition from snapshot
        net_string = snapshot["model"]["petri_net_definition"]
        net, im, fm = pm4py.objects.petri.importer.import_petri_from_string(net_string)

        # Identify missing activities
        missing_acts = identify_missing_activities(log, net)
        if missing_acts:
            print(f"Found missing activities: {missing_acts}")
            # Repair the net by adding new activities
            net, im, fm = repair_petri_net(net, im, fm, missing_acts)

            # Re-compute fitness with the repaired model
            updated_fitness = fitness_evaluator.apply(log, net, im, fm)
            print(f"Updated fitness: {updated_fitness}")

            # Update snapshot with new model & fitness
            net_string = pm4py.objects.petri.exporter.to_string(net)
            snapshot["model"]["petri_net_definition"] = net_string
            snapshot["analysis_results"]["fitness"] = updated_fitness
        else:
            print("No missing activities found. Model might need a different re-discovery approach.")
    else:
        print("Fitness is acceptable -> No rework needed.")

    return snapshot


# --------------- Main Workflow ---------------
def process_log(file_path):
    """
    End-to-end process analysis pipeline.
    1) Load log
    2) Discover model
    3) Compute fitness
    4) Analyze variants
    5) Conformance check
    6) Create snapshot
    7) Repair model if needed
    """
    # Step 1: Load XES log
    log = load_xes_log(file_path)

    # Step 2: Discover Petri net
    net, im, fm = discover_process_model(log)

    # Step 3: Compute fitness
    fitness_result = compute_fitness(log, net, im, fm)

    # Step 4: Analyze variants
    variant_result = analyze_variants(log)

    # Step 5: Perform conformance checking
    conformance_results = perform_conformance_check(log, net, im, fm)

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
        conformance_results=conformance_results
    )

    # Step 7: Attempt to repair the model if fitness is too low
    snapshot = rework_model_if_needed(snapshot, log)

    print("Process analysis completed.\n")
    return snapshot


# --------------- Execution ---------------
if __name__ == "__main__":
    FILE_PATH = "example.xes"  # Replace with your XES file path
    final_snapshot = process_log(FILE_PATH)
    print("Final Snapshot:", json.dumps(final_snapshot, indent=4))
