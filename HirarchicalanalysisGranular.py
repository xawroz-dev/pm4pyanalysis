import json
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.util import xes_constants as xes

from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.visualization.process_tree import visualizer as pt_vis
# Read the JSON data from the file
with open('granular_event_log.json', 'r') as file:
    data = json.load(file)
import os
os.environ["PATH"] += os.pathsep + 'C:\\Program Files\\Graphviz\\bin'
# Flatten the events
def flatten_events(case):
    events = []
    for event in case['events']:
        event['case_id'] = case['case_id']
        events.append(event)
    return events

# Process all cases
all_events = []
for case in data:
    events = flatten_events(case)
    all_events.extend(events)


# Create a DataFrame
df = pd.DataFrame(all_events)
df = df.drop(columns='event_id')
print(df.columns)
print(df)

# Preprocess the DataFrame
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = dataframe_utils.convert_timestamp_columns_in_df(df)
df = df.sort_values(by=['case_id', 'timestamp']).reset_index(drop=True)

# Prepare DataFrame for PM4Py
df = df.rename(columns={
    'case_id': 'case:concept:name',
    'activity': 'concept:name',
    'timestamp': 'time:timestamp'
})
print(df.columns)

# Perform Process Mining on Granular Data
log = log_converter.apply(df)
tree = inductive_miner.apply(log)
gviz = pt_vis.apply(tree)
pt_vis.view(gviz)

# Define aggregation mapping
aggregation_mapping = {
    'Validate Username': 'Authentication',
    'Validate Password': 'Authentication',
    'Check Permissions': 'Authorization',
    'Load Dashboard': 'Dashboard Access',
    # Add more mappings as needed
}

# Apply the aggregation mapping
df_aggregated = df.copy()
df_aggregated['concept:name'] = df_aggregated['concept:name'].replace(aggregation_mapping)

# Perform Process Mining on Aggregated Data
log_aggregated = log_converter.apply(df_aggregated)
tree_aggregated = inductive_miner.apply(log_aggregated)
gviz_aggregated = pt_vis.apply(tree_aggregated)
pt_vis.view(gviz_aggregated)
