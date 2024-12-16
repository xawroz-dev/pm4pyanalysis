from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import factory as xes_exporter
import pandas as pd

# Load the generated CSV event log
df = pd.read_csv("synthetic_event_log.csv")

# Convert the DataFrame columns for PM4py compatibility
df = dataframe_utils.convert_timestamp_columns_in_df(df)
df = dataframe_utils.rename_columns(df)

# Convert the DataFrame to an EventLog
event_log = log_converter.apply(df)

# Export the EventLog to an XES file
xes_exporter.apply(event_log, "synthetic_event_log.xes")
print("Event log successfully exported to XES format.")
