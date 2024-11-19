import pandas as pd
import numpy as np
from pm4py import get_variants
from pm4py.algo.discovery.inductive.algorithm import Variants
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.objects.log.util import dataframe_utils
from pm4py.statistics.traces.generic.log import case_statistics
from pm4py.util import xes_constants as xes
from pm4py.objects.conversion.process_tree import converter as pt_converter
import matplotlib.pyplot as plt




from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.visualization.process_tree import visualizer as pt_vis
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.visualization.bpmn import visualizer as bpmn_vis
from pm4py.algo.discovery.inductive import algorithm as inductive_miner_bpmn
from pm4py.algo.discovery.inductive import algorithm as inductive_miner_petri
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.visualization.heuristics_net import visualizer as hn_vis
from pm4py.visualization.petri_net import visualizer as process_map_vis
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
## Import the dfg visualization object
from pm4py.visualization.dfg import visualizer as dfg_visualization

from pm4py.visualization.dfg import visualizer as dfg_vis
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery

from pm4py.visualization.petri_net import visualizer as petri_vis

import pandas as pd
import numpy as np

import os


os.environ["PATH"] += os.pathsep + 'C:\\Program Files\\Graphviz\\bin'
# Define the number of cases and events
num_cases = 50

# Define activities at different levels
activities = {
    'activity': ['Process Order', 'Handle Return'],
    'subactivity': {
        'Process Order': ['Validate Order', 'Process Payment', 'Ship Order'],
        'Handle Return': ['Receive Return', 'Inspect Item', 'Process Refund']
    },
    'subsubactivity': {
        'Validate Order': ['Check Inventory', 'Verify Customer Details'],
        'Process Payment': ['Authorize Payment', 'Capture Payment'],
        'Ship Order': ['Pack Item', 'Schedule Shipment'],
        'Receive Return': ['Acknowledge Return', 'Arrange Pickup'],
        'Inspect Item': ['Assess Damage', 'Update Inventory'],
        'Process Refund': ['Calculate Refund', 'Issue Refund']
    }
}

# Initialize an empty list to hold event data
data = []

# Generate synthetic event logs
np.random.seed(42)  # For reproducibility


for case_id in range(1, num_cases + 1):
    timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(days=case_id)

    # Randomly choose a high-level activity
    activity = np.random.choice(activities['activity'], p=[0.8, 0.2])

    # Shuffle subactivities to introduce variation
    subactivities = np.random.permutation(activities['subactivity'][activity])

    for subactivity in subactivities:
        timestamp += pd.Timedelta(hours=1)

        # Check if sub-subactivities exist for the current subactivity
        if subactivity in activities['subsubactivity']:
            # Shuffle sub-subactivities to introduce variation
            subsubactivities = np.random.permutation(activities['subsubactivity'][subactivity])
            for subsubactivity in subsubactivities:
                timestamp += pd.Timedelta(minutes=30)
                # Combine all levels into a single event
                data.append({
                    'case_id': f'Case_{case_id}',
                    'activity': activity,
                    'subactivity': subactivity,
                    'subsubactivity': subsubactivity,
                    'timestamp': timestamp,
                    'event_id': f'Event_{case_id}_{activity}_{subactivity}_{subsubactivity}'
                })
        else:
            # If no sub-subactivities, create a single event for the subactivity
            data.append({
                'case_id': f'Case_{case_id}',
                'activity': activity,
                'subactivity': subactivity,
                'subsubactivity': None,
                'timestamp': timestamp,
                'event_id': f'Event_{case_id}_{activity}_{subactivity}'
            })

def visaulize_diagrams(log):

    # Apply Inductive Miner
    tree_process_payment = inductive_miner.apply(log)

    # Visualize Process Tree
    gviz_process_payment = pt_vis.apply(tree_process_payment)
    pt_vis.view(gviz_process_payment)




    # Discover Petri net using Alpha Miner
    net_alpha, im_alpha, fm_alpha = alpha_miner.apply(log)

    # Visualize process map
    gviz_map = process_map_vis.apply(net_alpha, im_alpha, fm_alpha)
    process_map_vis.view(gviz_map)



    # Discover directly-follows graph
    dfg = dfg_discovery.apply(log)

    # Visualize directly-follows graph
    gviz_dfg = dfg_vis.apply(dfg)
    dfg_vis.view(gviz_dfg)

    # Discover Petri net
    net, initial_marking, final_marking = alpha_miner.apply(log)

    # Visualize Petri Net
    gviz_petri = petri_vis.apply(net, initial_marking, final_marking)
    petri_vis.view(gviz_petri)

    # Apply Inductive Miner to discover a BPMN model
    tree = inductive_miner.apply(log, variant=inductive_miner.Variants.IM)

    # Step 2: Convert ProcessTree to BPMN
    bpmn_graph = pt_converter.apply(tree, variant=pt_converter.Variants.TO_BPMN)

    # Step 3: Visualize BPMN
    gviz_bpmn = bpmn_vis.apply(bpmn_graph)
    bpmn_vis.view(gviz_bpmn)

    ## Import the dfg_discovery algorithm

    # Create graph from log
    dfg = dfg_discovery.apply(log)
    # Visualise
    gviz = dfg_visualization.apply(dfg, log=log, variant=dfg_visualization.Variants.FREQUENCY)
    dfg_visualization.view(gviz)

    # heu_net_tuple = heuristics_miner.apply_heu(log)
    #
    # # Debug: Print the tuple content
    # print(f"Returned tuple: {heu_net_tuple}")
    #
    # # The first element of the tuple should be the heuristics net
    # heu_net = heu_net_tuple[0]  # Adjust index if needed
    #
    # # Visualize Heuristics Net
    # gviz_heu = hn_vis.apply(heu_net_tuple)
    # hn_vis.view(gviz_heu)



def view_variants(log):
    # Perform variant analysis

    variants = get_variants(log)
    variant_count = len(variants)
    print(f": {variant_count}")
    variants_count = case_statistics.get_variant_statistics(log)
    variants_count = sorted(variants_count, key=lambda x: x['count'], reverse=True)

    # Count frequency of each variant
    variant_frequencies = {variant: len(cases) for variant, cases in variants.items()}
    sorted_variants = sorted(variant_frequencies.items(), key=lambda x: x[1], reverse=True)

    # Show the top 5 variants
    print("Top 5 variants:")
    for i, (variant, count) in enumerate(sorted_variants[:5], start=1):
        print(f"{i}. {variant}: {count} cases")

    # Generate a graph for the variants
    most_common_variant = variants_count[0]['variant']
    log_variant = variants_filter.apply(log, [most_common_variant])

    # Perform mining on the variant
    print(f"\nProcess Mining for the Most Common Variant:")
    net, initial_marking, final_marking = alpha_miner.apply(log)

    gviz_variant = petri_vis.apply(net, initial_marking, final_marking)
    petri_vis.view(gviz_variant)

    variant_names = [str(variant) for variant, _ in sorted_variants]
    variant_counts = [count for _, count in sorted_variants]

    plt.figure(figsize=(10, 6))
    plt.bar(variant_names[:10], variant_counts[:10], color='skyblue')
    plt.xlabel('Variants', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.title(f'Top 10 Variants', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()



# Convert to DataFrame
df = pd.DataFrame(data)


print(df.columns)


# Convert timestamp columns
df = dataframe_utils.convert_timestamp_columns_in_df(df)


output_file = "output_event_log.json"
df.to_json(output_file, orient="records", lines=False, date_format="iso")

# Step 4: Define perform_mining function
def perform_mining(df, level):
    print(df.columns)
    level_to_column = {
        1: 'activity',
        2: 'subactivity',
        3: 'subsubactivity'
    }
    if level not in level_to_column:
        raise ValueError("Invalid level. Must be 1 (activity), 2 (subactivity), or 3 (subsubactivity).")

        # Select the appropriate column based on the level
    selected_column = level_to_column[level]

    # Create a filtered DataFrame
    df_filtered = df.copy()
    df_filtered = df_filtered.rename(columns={
        'case_id': 'case:concept:name',
        selected_column: 'concept:name',  # Dynamically set the concept:name column
        'timestamp': 'time:timestamp'
    })
    # df_filtered = df_filtered.dropna(subset=['concept:name'])

    # print(df_filtered.columns)
    log_filtered = log_converter.apply(df_filtered)
    print(f"Number of unique variants at level {level}: ")
    view_variants(log_filtered)
    visaulize_diagrams(log_filtered)


# Step 5: Perform Mining at Different Levels
# print("Process Mining at Level 1 (Generic Analysis):")
# perform_mining(df, level=1)
#
# print("Process Mining at Level 2 (Mid-Level Analysis):")
# perform_mining(df, level=2)

print("Process Mining at Level 3 (Detailed Analysis):")
perform_mining(df, level=3)

# Step 6: Hierarchical Abstraction
# Define abstraction mapping
abstraction_mapping = {
    'Check Inventory': 'Validate Order',
    'Verify Customer Details': 'Validate Order',
    'Authorize Payment': 'Process Payment',
    'Capture Payment': 'Process Payment',
    'Pack Item': 'Ship Order',
    'Schedule Shipment': 'Ship Order',
    'Assess Damage': 'Inspect Item',
    'Update Inventory': 'Inspect Item',
    'Calculate Refund': 'Process Refund',
    'Issue Refund': 'Process Refund',
    'Acknowledge Return': 'Receive Return',
    'Arrange Pickup': 'Receive Return'
}
df_abstracted = df.copy()

# Apply abstraction mapping to 'concept:name'
df_abstracted['concept:name'] = df_abstracted['subsubactivity'].replace(abstraction_mapping)

df_abstracted = df_abstracted.rename(columns={
    'case_id': 'case:concept:name',
    'activity': 'concept:name',
    'timestamp': 'time:timestamp'
})

# Convert DataFrame to Event Log
log_abstracted = log_converter.apply(df_abstracted)

visaulize_diagrams(log_abstracted)


# Step 7: Drilling Down into Subprocesses
# Filter events where 'parent_activity' is 'Process Payment' or 'activity' is 'Process Payment'
df_process_payment = df[
    (df['activity'] == 'Process Payment')].copy()

# Rename columns
df_process_payment = df_process_payment.rename(columns={
    'case_id': 'case:concept:name',
    'subactivity': 'concept:name',
    'timestamp': 'time:timestamp'
})

# Convert DataFrame to Event Log
log_process_payment = log_converter.apply(df_process_payment)
visaulize_diagrams(log_process_payment)

