
import pandas as pd
import random
from datetime import datetime, timedelta
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.visualization.process_tree import visualizer as pt_visualizer
from pm4py.visualization.dfg import visualizer as dfg_visualizer
from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness
from pm4py.algo.evaluation.precision import algorithm as precision
from pm4py.algo.evaluation.generalization import algorithm as generalization
from pm4py.algo.evaluation.simplicity import algorithm as simplicity
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
from pm4py.statistics.variants.log import get as get_variants
from pm4py.util.variants_util import get_variant_from_trace
from pm4py.stats import get_trace_attributes
from pm4py.util import constants
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
from pm4py.objects.petri_net.obj import PetriNet
from pm4py.algo.simulation.playout.petri_net import algorithm as petri_net_playout

# Helper function to generate random timestamps
def random_timestamp(base_time, max_duration=10):
    return base_time + timedelta(seconds=random.randint(0, max_duration))

# Define process instances with parallel, cyclic, and asynchronous activities
def generate_event_log():
    data = []
    parallel_activities = [f"Parallel_{i}" for i in range(1, 10)]  # 8-9 parallel activities

    for i in range(1, 11):  # 10 process instances
        process_id = f"case_{i}"
        start_time = datetime(2024, 12, 1, 8, 0) + timedelta(hours=i)
        current_time = start_time

        # Sequential activity
        data.append([process_id, "Start", current_time, "start"])
        current_time = random_timestamp(current_time)

        # Randomize the order of parallel activities
        randomized_parallel_activities = random.sample(parallel_activities, len(parallel_activities))
        for activity in randomized_parallel_activities:
            data.append([process_id, activity, random_timestamp(current_time), "parallel"])
        current_time = random_timestamp(current_time)

        # Cyclic activity
        for j in range(random.randint(1, 3)):  # Add a cycle
            data.append([process_id, f"Cyclic_{j + 1}", random_timestamp(current_time), "cyclic"])
        current_time = random_timestamp(current_time)

        # Asynchronous activity
        async_call_time = random_timestamp(current_time)
        data.append([process_id, "Async_Call_Start", async_call_time, "async"])
        data.append([process_id, "Async_Call_End", random_timestamp(async_call_time, 15), "async"])

        # Error boundary (optional activity)
        if random.random() > 0.7:  # 30% chance of error
            data.append([process_id, "Error_Handler", random_timestamp(current_time), "error"])

        # End activity
        data.append([process_id, "End", random_timestamp(current_time), "end"])

    return pd.DataFrame(data, columns=["case_id", "activity", "timestamp", "type"])

# Generate the synthetic event log
event_log = generate_event_log()

# Convert to PM4Py Event Log
event_log = dataframe_utils.convert_timestamp_columns_in_df(event_log)
event_log = event_log.sort_values("timestamp")
event_log = event_log.rename(columns={"case_id": "case:concept:name", "activity": "concept:name", "timestamp": "time:timestamp"})
log = log_converter.apply(event_log)

# Process Mining
models = {}

# Alpha Miner
net, initial_marking, final_marking = alpha_miner.apply(log)
models["alpha"] = (net, initial_marking, final_marking)

from pm4py.objects.conversion.process_tree import converter as pt_converter

# Inductive Miner (returning a Petri net)
tree = inductive_miner.apply(log)
net, im, fm = pt_converter.apply(tree)
models["inductive"] = (net, im, fm)

# Heuristics Miner (returning a Petri net)
net, im, fm = heuristics_miner.apply(log)
models["heuristics"] = (net, im, fm)

# Conformance Checking
conformance = {}
from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
from pm4py.convert import convert_to_bpmn

for model_name, (net, initial_marking, final_marking) in models.items():
    print(f"Visualizing {model_name.upper()} model...")
    # Visualize Petri net
    # gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    # pn_visualizer.view(gviz)


    # Convert to BPMN and visualize
    try:
        bpmn_model_from_tree = convert_to_bpmn(net, initial_marking, final_marking)
        bpmn_visualizer.apply(bpmn_model_from_tree, variant=bpmn_visualizer.Variants.CLASSIC)

        # Visualize the BPMN model
        # gviz_bpmn = bpmn_visualizer.apply(bpmn_model)
        # bpmn_visualizer.view(gviz_bpmn)
    except Exception as e:
        print(f"Could not convert {model_name} to BPMN: {e}")
    conformance[model_name] = {}

    # Token Replay (for all models)
    try:
        token_replay_result = token_replay.apply(log, net, initial_marking, final_marking)

        # Extract trace fitnesses
        trace_fitnesses = [trace["trace_fitness"] for trace in token_replay_result if "trace_fitness" in trace]
        average_fitness = sum(trace_fitnesses) / len(trace_fitnesses) if trace_fitnesses else 0.0
        traces_with_problems = len([trace for trace in token_replay_result if trace.get("trace_fitness", 1.0) < 1.0])

        conformance[model_name]["token_replay_fitness"] = average_fitness
        conformance[model_name]["token_replay_traces_with_problems"] = traces_with_problems
    except Exception as e:
        print(f"Token Replay failed for {model_name}: {e}")
        conformance[model_name]["token_replay_error"] = str(e)

# Print Conformance Checking Results
print("\nConformance Checking:")
for model_name, metrics in conformance.items():
    print(f"\n{model_name.upper()} Model:")
    for metric_name, metric_value in metrics.items():
        print(f"  {metric_name.replace('_', ' ').title()}: {metric_value}")

# Unique variant models
unique_variant_models = []  # Will store process models for unique variants


# Check if a variant can be replayed on an existing process model
def can_replay_on_model(variant, model):
    """
    Checks if the variant can be replayed on a given process model.
    """
    net, initial_marking, final_marking = model
    # Create a temporary log for the variant
    temp_log = []
    for i, activity in enumerate(variant):
        temp_log.append({"case:concept:name": "temp_case", "concept:name": activity,
                         "time:timestamp": datetime(2024, 1, 1, 0, 0) + timedelta(seconds=i)})
    temp_log = log_converter.apply(pd.DataFrame(temp_log))
    # Replay the log against the model
    try:
        replay_result = token_replay.apply(temp_log, net, initial_marking, final_marking)
        # Check if all traces fit perfectly
        return all(fitness["trace_fitness"] == 1.0 for fitness in replay_result)
    except Exception as e:
        return False


from pm4py.util.constants import PARAMETER_CONSTANT_ACTIVITY_KEY
from pm4py.statistics.variants.log import get as variants_get

# Get Variants
variants = variants_get.get_variants(log, parameters={PARAMETER_CONSTANT_ACTIVITY_KEY: "concept:name"})

# Print variants
print("\nVariants:")
for variant, traces in variants.items():
    print(f"Variant: {variant}, Occurrences: {len(traces)}")

# Loop through each variant
for variant, traces in variants.items():
    # Get the activities in this variant
    activities = list(variant)  # Convert the tuple to a list

    # Check if the variant fits on any existing model
    is_new_variant = True
    for model in unique_variant_models:
        if can_replay_on_model(activities, model):
            is_new_variant = False
            break

    # If it doesn't fit on any model, create a new model for it
    if is_new_variant:
        # Create an event log for this variant
        variant_log = []
        for trace in traces:
            for i, event in enumerate(trace):
                variant_log.append({
                    "case:concept:name": f"case_{len(unique_variant_models) + 1}",
                    "concept:name": event["concept:name"],
                    "time:timestamp": datetime(2024, 1, 1, 0, 0) + timedelta(seconds=i)
                })
        variant_log = log_converter.apply(pd.DataFrame(variant_log))

        try:
            # Apply Inductive Miner to generate a process tree
            process_tree = inductive_miner.apply(variant_log)

            # Convert the process tree to a Petri net
            net, initial_marking, final_marking = pt_converter.apply(process_tree)

            unique_variant_models.append((net, initial_marking, final_marking))
        except Exception as e:
            print(f"Failed to generate process model for variant: {e}")

# Visualize all unique models
print(f"\nNumber of unique variants: {len(unique_variant_models)}")
# for idx, model in enumerate(unique_variant_models, 1):
#     print(f"\nVisualizing model for unique variant {idx}...")
#     net, im, fm = model
#     gviz = pn_visualizer.apply(net, im, fm)
#     pn_visualizer.view(gviz)