import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.log.obj import EventLog
from pm4py.algo.conformance.alignments import factory as align_factory
from pm4py.objects.petri_net.importer import factory as pnml_importer
from pm4py.algo.discovery.alpha import factory as alpha_miner
from pm4py.statistics.performance_diagnostics import algorithm as performance_analysis


# Example of a DataFrame with an event log structure
df = pd.read_csv('synthetic_event_log.csv')

# Convert timestamp columns to datetime
df = dataframe_utils.convert_timestamp_columns_in_df(df)

# Rename columns to the expected format
df = dataframe_utils.rename_columns(df)

# Convert DataFrame to EventLog

# Convert the DataFrame into an event log compatible with PM4py
log = dataframe_utils.convert_to_event_log(df)
# Discover a process model using the Alpha Miner
net, initial_marking, final_marking = alpha_miner.apply(log)

# Visualize the process model
from pm4py.visualization.petri_net import factory as vis_factory
gviz = vis_factory.apply(net, initial_marking, final_marking)
vis_factory.view(gviz)

# Performance analysis
performance_metrics = performance_analysis.apply(log, net, initial_marking, final_marking)
print("Performance Metrics:")
print(performance_metrics)

# Conformance checking
alignments = align_factory.apply_log(log, net, initial_marking, final_marking)
print("Alignments:")
for case in alignments:
    print(case)

# Variant-based conformance check
from pm4py.statistics.variants.log import get as variants_get
variants = variants_get.get_variants(log)
most_common_variant = max(variants, key=lambda k: len(variants[k]))
least_common_variant = min(variants, key=lambda k: len(variants[k]))
print("Most Common Variant:", most_common_variant)
print("Least Common Variant:", least_common_variant)

# Conformance check between most and least common variants
most_common_cases = variants[most_common_variant]
least_common_cases = variants[least_common_variant]

# Filter log for specific variants
from pm4py.algo.filtering.log.variants import variants_filter
filtered_log_common = variants_filter.apply(log, most_common_cases)
filtered_log_least = variants_filter.apply(log, least_common_cases)

# Alignment for the most and least common variants
alignment_common = align_factory.apply_log(filtered_log_common, net, initial_marking, final_marking)
alignment_least = align_factory.apply_log(filtered_log_least, net, initial_marking, final_marking)

print("Alignment for Most Common Variant:")
print(alignment_common)
print("Alignment for Least Common Variant:")
print(alignment_least)
