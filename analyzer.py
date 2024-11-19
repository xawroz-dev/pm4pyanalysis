import json
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.algo.discovery import alpha, heuristics, inductive
from pm4py.visualization.petrinet import factory as pn_vis
from pm4py.visualization.process_tree import factory as pt_vis
from pm4py.visualization.bpmn import visualizer as bpmn_vis
from pm4py.objects.conversion.petri_to_bpmn import converter as pn_to_bpmn_converter
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.statistics.traces.log import case_statistics
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.util import xes_constants as xes
import matplotlib.pyplot as plt

# Step 1: Load JSON Data
with open('event_log.json', 'r') as f:
    json_data = json.load(f)

# Step 2: Flatten JSON Data
def flatten_events(data):
    flattened_data = []

    def flatten(event, case_id):
        base_event = {
            'case_id': case_id,
            'event_id': event['event_id'],
            'activity': event['activity'],
            'timestamp': event['timestamp'],
            'event_level': event['event_level'],
        }
        flattened_data.append(base_event)

        if 'sub_activities' in event:
            for sub_event in event['sub_activities']:
                flatten(sub_event, case_id)

    for record in data:
        flatten(record, record['case_id'])

    return pd.DataFrame(flattened_data)

df = flatten_events(json_data)

# Step 3: Preprocess Data
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values(['case_id', 'timestamp']).reset_index(drop=True)
df = dataframe_utils.convert_timestamp_columns_in_df(df)
df = df.rename(columns={
    'case_id': xes.DEFAULT_CASEID_KEY,
    'activity': xes.DEFAULT_NAME_KEY,
    'timestamp': xes.DEFAULT_TIMESTAMP_KEY
})

# Step 4: Perform Process Mining at Different Levels
def perform_process_mining(df, level):
    # Filter data for the specified level
    df_level = df[df['event_level'] == level]
    if df_level.empty:
        print(f"No events at level {level}.")
        return

    # Convert DataFrame to Event Log
    log = log_converter.apply(df_level)

    # Apply Inductive Miner
    net, initial_marking, final_marking = inductive.apply(log)

    # Visualize Petri net
    gviz = pn_vis.apply(net, initial_marking, final_marking)
    pn_vis.view(gviz)

    # Convert to BPMN
    bpmn_model = pn_to_bpmn_converter.apply(net, initial_marking, final_marking)
    gviz_bpmn = bpmn_vis.apply(bpmn_model)
    bpmn_vis.view(gviz_bpmn)

    # Visualize Process Tree
    process_tree = inductive.apply_tree(log)
    gviz_tree = pt_vis.apply(process_tree)
    pt_vis.view(gviz_tree)

# Perform mining for each level
for level in range(1, 4):
    print(f"\nPerforming process mining for Level {level} events:")
    perform_process_mining(df, level)

# Step 5: Generalized Top View (Level 1)
print("\nGeneralized Top-Level View:")
perform_process_mining(df, level=1)

# Step 6: Detailed Granular View (All Levels)
print("\nDetailed Granular View (All Levels):")
log_all = log_converter.apply(df)
net_all, im_all, fm_all = inductive.apply(log_all)
gviz_all = pn_vis.apply(net_all, im_all, fm_all)
pn_vis.view(gviz_all)

# Convert to BPMN
bpmn_model_all = pn_to_bpmn_converter.apply(net_all, im_all, fm_all)
gviz_bpmn_all = bpmn_vis.apply(bpmn_model_all)
bpmn_vis.view(gviz_bpmn_all)

# Step 7: Analyze Variants
variants_count = case_statistics.get_variant_statistics(log_all)
variants_count = sorted(variants_count, key=lambda x: x['count'], reverse=True)

# Display variants
print("\nVariants:")
for variant in variants_count:
    print(f"Variant: {variant['variant']}, Count: {variant['count']}")

# Step 8: Visualize a Specific Variant
most_common_variant = variants_count[0]['variant']
log_variant = variants_filter.apply(log_all, [most_common_variant])

# Perform mining on the variant
print(f"\nProcess Mining for the Most Common Variant:")
net_variant, im_variant, fm_variant = inductive.apply(log_variant)
gviz_variant = pn_vis.apply(net_variant, im_variant, fm_variant)
pn_vis.view(gviz_variant)

# Optionally, you can save the visualizations
# pn_vis.save(gviz_variant, "variant_petri_net.png")
# bpmn_vis.save(gviz_bpmn_all, "all_levels_bpmn.png")
