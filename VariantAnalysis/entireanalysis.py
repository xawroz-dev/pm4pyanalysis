import pandas as pd
import random
from datetime import datetime, timedelta
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer
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
import os

# Helper function to generate random timestamps
def random_timestamp(base_time, max_duration=10):
    return base_time + timedelta(seconds=random.randint(0, max_duration))

# Define process instances with parallel, cyclic, and asynchronous activities
def generate_event_log():
    # ... (Existing event log generation code)
    data = []
    parallel_activities = [f"Parallel_{i}" for i in range(1, 10)]  # 8-9 parallel activities

    for i in range(1, 10):  # 10 process instances
        process_id = f"case_{i}"
        start_time = datetime(2024, 12, 1, 8, 0) + timedelta(hours=i)
        current_time = start_time

        # Sequential activity
        data.append([process_id, "Start", current_time, "start"])
        current_time = random_timestamp(current_time)

        # Randomize the order of parallel activities
        randomized_parallel_activities = random.sample(parallel_activities, len(parallel_activities))
        for activity in randomized_parallel_activities:
            data.append([process_id, activity, current_time, "parallel"])
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

# Convert to XES and PM4Py Event Log
event_log = dataframe_utils.convert_timestamp_columns_in_df(event_log)
event_log = event_log.sort_values("timestamp")
event_log = event_log.rename(columns={"case_id": "case:concept:name", "activity": "concept:name", "timestamp": "time:timestamp"})

log = log_converter.apply(event_log)
log_converter.apply(log, "synthetic_event_log.xes")

# Process Mining and Evaluation
models = {}
evaluations = {}

# Alpha Miner
net, initial_marking, final_marking = alpha_miner.apply(log)
models["alpha"] = (net, initial_marking, final_marking)
gviz = pn_visualizer.apply(net, initial_marking, final_marking)
pn_visualizer.view(gviz)

# Inductive Miner
tree = inductive_miner.apply(log)
models["inductive"] = tree
gviz = pt_visualizer.apply(tree)
pt_visualizer.view(gviz)

# Heuristics Miner
heu_net = heuristics_miner.apply_heu(log)
models["heuristics"] = heu_net
net, im, fm = heuristics_miner.apply(log)
# Petri net visualisation
gviz = pn_visualizer.apply(net, im, fm)
pn_visualizer.view(gviz)

# Evaluation (using replay fitness, precision, generalization, simplicity)
for model_name, model in models.items():
    evaluations[model_name] = {}
    if model_name == "alpha":
        net, initial_marking, final_marking = model
        evaluations[model_name]["fitness"] = replay_fitness.apply(log, net, initial_marking, final_marking)
        evaluations[model_name]["precision"] = precision.apply(log, net, initial_marking, final_marking)
        evaluations[model_name]["generalization"] = generalization.apply(log, net, initial_marking, final_marking)
        evaluations[model_name]["simplicity"] = simplicity.apply(net)
    #Inductive miner evaluation
    # elif model_name == "inductive":
    #     from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
    #
    #     net, initial_marking, final_marking = model
    #
    #     evaluations[model_name]["fitness"] = token_replay.apply(log, net, initial_marking, final_marking)
    #     evaluations[model_name]["precision"] = precision.apply(log, model)
    #     evaluations[model_name]["generalization"] = generalization.apply(log, model)
    #     evaluations[model_name]["simplicity"] = simplicity.apply(model)
    #Heuristic miner evaluation
    elif model_name == "heuristics":
        net, initial_marking, final_marking = heuristics_miner.apply(log)
        evaluations[model_name]["fitness"] = replay_fitness.apply(log, net, initial_marking, final_marking)
        evaluations[model_name]["precision"] = precision.apply(log, net, initial_marking, final_marking)
        evaluations[model_name]["generalization"] = generalization.apply(log, net, initial_marking, final_marking)
        evaluations[model_name]["simplicity"] = simplicity.apply(net)

# Print Evaluation Results
print("\nModel Evaluation:")
for model_name, metrics in evaluations.items():
    print(f"\n{model_name.upper()} Model:")
    for metric_name, metric_value in metrics.items():
        print(f"  {metric_name.capitalize()}: {metric_value}")
#
# Process Mining and Visualization
# net, initial_marking, final_marking = alpha_miner.apply(log)
# gviz = pn_visualizer.apply(net, initial_marking, final_marking)
# pn_visualizer.save(gviz, "alpha_model.png")
#
# tree = inductive_miner.apply(log)
# gviz = pt_visualizer.apply(tree)
# pt_visualizer.save(gviz, "inductive_model.png")
# from pm4py.objects.process_tree.obj import ProcessTree
# from pm4py.objects.conversion.process_tree import converter as pt_converter
#
# heu_net = heuristics_miner.apply_heu(log)
# models["heuristics"] = heu_net
# net, im, fm = heuristics_miner.apply(log)
# # Petri net visualisation
# gviz = pn_visualizer.apply(net, im, fm)
#
# # Evaluation
# fitness = replay_fitness.apply(log, net, initial_marking, final_marking)
# prec = precision.apply(log, net, initial_marking, final_marking)
# gen = generalization.apply(log, net, initial_marking, final_marking)
# simp = simplicity.apply(net)
#
# print(f"Fitness: {fitness}, Precision: {prec}, Generalization: {gen}, Simplicity: {simp}")
#
# # Conformance Checking
# aligned_traces = alignments.apply(log, net, initial_marking, final_marking)
#
# # Variant Analysis
# variants = get_variants(log)
#
# print("\nVariants:")
# for variant, traces in variants.items():
#     print(f"Variant: {variant}, Count: {len(traces)}")
#
# # Example of checking if a variant is a subset of another
# variants_list = list(variants.keys())
# if len(variants_list) > 1:
#     if set(variants_list[0]).issubset(set(variants_list[1])):
#         print(f"Variant {variants_list[0]} is a subset of Variant {variants_list[1]}")
#     else:
#         print(f"Variant {variants_list[0]} is NOT a subset of Variant {variants_list[1]}")
#
# # Example of getting a variant from a trace
# example_trace = log[0]
# variant_of_trace = get_variant_from_trace(example_trace)
# print(f"\nVariant of the first trace: {variant_of_trace}")
#
# # Get trace attributes
# trace_attributes = get_trace_attributes(log)
# print(f"\nTrace Attributes: {trace_attributes}")