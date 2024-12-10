import pm4py
from pm4py.statistics.traces.generic.log import case_statistics
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness
from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
# from pm4py.algo.enhancement.performance_spectrum import algorithm as performance_spectrum
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.visualization.performance_spectrum import visualizer as ps_visualizer
from pm4py.algo.filtering.log.attributes import attributes_filter
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py import get_variants
# Load the CSV as a PM4Py event log

df = pd.read_csv("hierarchical_metadata_event_log.csv")

# Rename columns to PM4PY's expected keys if necessary
df.rename(columns={
    "case_id": "case:concept:name",
    "activity": "concept:name",
    "timestamp": "time:timestamp",
    "lifecycle": "lifecycle:transition"
}, inplace=True)

# Convert the timestamp column to datetime format
df = dataframe_utils.convert_timestamp_columns_in_df(df)

# Convert the DataFrame to a PM4PY Event Log
log = log_converter.apply(df)
# Load the CSV event log with hierarchical metadata

# Convert to dataframe for analysis
df = pm4py.convert_to_dataframe(log)
print(df)
# log_complete = attributes_filter.apply_events(log, ["complete"], parameters={"attribute_key": "lifecycle:transition"})
# print(log_complete)
df_complete = df[df['lifecycle:transition'] == 'complete']
print(df_complete)
log_complete = log_converter.apply(df_complete)
# ----------------------------------
# TOP-LEVEL DISCOVERY (Just use level_1)
# Filter to get only the "start" and "complete" events of top-level steps (We have them anyway, but let's assume we want top-level)
# Actually, top-level events are those having a 'level_1' attribute set, but we won't filter out sub-level activities yet.
# Instead, we want to see only events with no deeper levels. Let's say top-level model: only events where level_2 is None.
top_level_activities_df = df_complete[df_complete["level_2"].isna()]  # only top-level activities have no level_2
log_top = pm4py.convert_to_event_log(top_level_activities_df)
tree = pm4py.discover_process_tree_inductive(log_top)
net_top, im_top, fm_top = pm4py.convert_to_petri_net(tree)
# net_top, im_top, fm_top = inductive_miner.apply(log, variant=inductive_miner.Variants.IM)

pm4py.view_petri_net(net_top, im_top, fm_top)

# This shows a very high-level process: A, B, C, D steps.

# ----------------------------------
# DRILL DOWN INTO A SPECIFIC LEVEL_1 STEP (e.g., "A")
# Filter events where level_1 == "A"
df_A = df_complete[df_complete['level_1'] == 'A']


log_A = log_converter.apply(df_A)
tree_A = pm4py.discover_process_tree_inductive(log_A)
net_A, im_A, fm_A = pm4py.convert_to_petri_net(tree_A)
# net_A, im_A, fm_A = inductive_miner.apply(log_A)
pm4py.view_petri_net(net_A, im_A, fm_A)

# Now we see the sub-steps of "A" including A1 and A2 sets of events.

# ----------------------------------
# DRILL DOWN FURTHER (e.g., within "A", look at A1 sub-steps only)
df_A1 = df_A[df_A['level_2'] == 'A1']


log_A1 = log_converter.apply(df_A1)

# log_A1 = attributes_filter.apply(log_A, [("level_2", "A1")], parameters={"positive": True})

tree_A1 = pm4py.discover_process_tree_inductive(log_A1)

net_A1, im_A1, fm_A1 = pm4py.convert_to_petri_net(tree_A1)
pm4py.view_petri_net(net_A1, im_A1, fm_A1)

# Now we have a model focusing on A.1 sub-level steps (like Check Card Number, Check Expiry, Check CVV).

# ----------------------------------
# PERFORMANCE AND CONFORMANCE ANALYSIS
# For performance analysis, we can look at case durations:
# ----------------------------------
# PERFORMANCE AND CONFORMANCE ANALYSIS

# ----------------------------------
# PERFORMANCE AND CONFORMANCE ANALYSIS
# ----------------------------------
# PERFORMANCE AND CONFORMANCE ANALYSIS

# Calculate case durations using the correct function
case_durations = case_statistics.get_all_case_durations(log)
# Compute the average case duration directly
average_case_duration = sum(case_durations) / len(case_durations) if case_durations else 0
print(f"Average Case Duration: {average_case_duration} seconds")

# Conformance Checking (Alignments) on the top-level model
alignments_result = alignments.apply_log(log_top, net_top, im_top, fm_top)
print("Alignment Results:")
print(alignments_result)

# Replay Fitness
fitness = replay_fitness.evaluate(log_top)
print(f"Top-level Replay Fitness: {fitness['averageFitness']}")

# Precision at the top-level (using ETConformance variant)
precision = precision_evaluator.apply(log_top, net_top, im_top, fm_top, variant=precision_evaluator.Variants.ETCONFORMANCE)
print(f"Top-level Precision: {precision}")

# ----------------------------------
# VARIANT ANALYSIS
# Retrieve variants from the log
variants_dict = case_statistics.get_variant_statistics(log, parameters={"case_id_key": "case:concept:name"})
# Sort variants by occurrence frequency
sorted_variants = sorted(variants_dict, key=lambda x: x['count'], reverse=True)

# Get the most and least common variants
most_common_variant = sorted_variants[0]
least_common_variant = sorted_variants[-1]

print(f"Most common variant: {most_common_variant['variant']} - Occurrences: {most_common_variant['count']}")
print(f"Least common variant: {least_common_variant['variant']} - Occurrences: {least_common_variant['count']}")

# Filter the log for the most common variant
log_mcv = attributes_filter.apply(log, [(most_common_variant['variant'])], parameters={
    "attribute_key": "concept:name",
    "positive": True
})
alignments_result_mcv = alignments.apply_log(log_mcv, net_top, im_top, fm_top)
fitness_mcv = replay_fitness.evaluate(alignments_result_mcv)
print(f"Most common variant Replay Fitness: {fitness_mcv['averageFitness']}")

# Filter the log for the least common variant
log_lcv = attributes_filter.apply(log, [(least_common_variant['variant'])], parameters={
    "attribute_key": "concept:name",
    "positive": True
})
alignments_result_lcv = alignments.apply_log(log_lcv, net_top, im_top, fm_top)
fitness_lcv = replay_fitness.evaluate(alignments_result_lcv)
print(f"Least common variant Replay Fitness: {fitness_lcv['averageFitness']}")
