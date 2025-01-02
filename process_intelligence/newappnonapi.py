import logging
from collections import defaultdict

import pm4py
from cortado_core.utils.alignment_utils import typed_trace_fits_process_tree
from cortado_core.utils.collapse_variants import collapse_variants
from flask import Flask, request, jsonify, session
from cortado_core.utils.cvariants import get_detailed_variants, get_concurrency_variants
from cortado_core.utils.split_graph import Group
from cortado_core.utils.timestamp_utils import TimeUnit
from pandas.core.interchange.dataframe_protocol import DataFrame
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.objects.log.obj import EventLog, Trace
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import xes as xes_utils
from pm4py.objects.log.util.interval_lifecycle import to_interval
from pm4py.util.xes_constants import DEFAULT_START_TIMESTAMP_KEY, DEFAULT_TRANSITION_KEY
from pm4py.objects.petri_net.exporter.exporter import pnml

from pm4py.discovery import discover_process_tree_inductive
#     variant_analysis,
#     process_discovery,
#     model_adjustment,
#     conformance_check,
# )
from flask_caching import Cache
from cortado_core.process_tree_utils.reduction import apply_reduction_rules

from add_variant_to_model import add_variants_to_process_model
from multithreading.pool_factory import PoolFactory
from process_tree_conversion import dict_to_process_tree
from utils import get_traces_from_variants


VARIANTS_KEY = "cached_variants"
PROCESS_MODEL_KEY = "cached_process_model"
VARIANT_NAMES_KEY = "variant_names"
LOG_KEY = "xes_log"



def get_variants(log):
    """
    Generates a dictionary of variants from the log.

    Args:
        log: The event log (in PM4Py format).

    Returns:
        A dictionary where keys are variant strings (e.g., "A,B,C") and values are lists of traces.
    """
    variants_dict = {}
    for trace in log:
        # Construct the variant string by concatenating activity names
        variant = ",".join([event["concept:name"] for event in trace])
        if variant not in variants_dict:
            variants_dict[variant] = []
        variants_dict[variant].append(trace)
    return variants_dict

def generate_variant_names(variants):
    """
    Generate names for variants and create a dictionary mapping variant names to activities.
    """
    variant_names = {}
    for i, variant in enumerate(variants):
        name = f"Variant_{i + 1}"
        variant_names[name] = variant["events"]
    return variant_names


def rename_event_attributes(event_log, activity_key='activity', new_key='concept:name'):
    """
    Renames attributes in an event log.

    Args:
        event_log: The event log (in PM4Py format).
        activity_key: The existing key for activity names.
        new_key: The new key to replace the old key.

    Returns:
        The modified event log with renamed attributes.
    """
    for trace in event_log:
        for event in trace:
            # Rename the activity key if it exists
            if activity_key in event:
                event[new_key] = event[activity_key]  # Add new key with old value
                del event[activity_key]  # Remove the old key
    return event_log


def get_variants():
    """
    API Endpoint: Get Variants
    --------------------------
    Description:
        Extracts variants from an XES log, generates names for the variants,
        and stores them in cache along with a dictionary mapping variant names to activities.

    Request Payload:
        - xes_log (str): The input XES log in string format.
        - collapse_variants (bool): (Optional) Whether to collapse similar variants. Default is False.

    Response:
        - variants (list): A list of identified variants with their event sequences.
        - variant_names (dict): A dictionary where keys are variant names (e.g., "Variant_1")
          and values are the corresponding event sequences.

    Example Request:
        {
            "xes_log": "<XES log string>",
            "collapse_variants": true
        }

    Example Response:
        {
            "variants": [
                {"variant_id": 1, "events": ["A", "B", "C"]},
                {"variant_id": 2, "events": ["A", "C", "D"]}
            ],
            "variant_names": {
                "Variant_1": ["A", "B", "C"],
                "Variant_2": ["A", "C", "D"]
            }
        }
    """
    global collapse_variants_list

    # data = request.get_json()
    xes_log = xes_importer.apply("/Users/sashrestha/PycharmProjects/process_intelligence/output.xes")
    xes_log_renamed = rename_event_attributes(xes_log, "activity", "concept:name")
    xes_log_renamed = rename_event_attributes(xes_log_renamed, "timestamp", "time:timestamp")
    xes_log_renamed = rename_event_attributes(xes_log, "case", "case:concept:name")
    # ev
    # if (
    #         DEFAULT_TRANSITION_KEY not in event_log[0][0]
    #         and DEFAULT_START_TIMESTAMP_KEY not in event_log[0][0]
    # ):
    #     event_log = to_interval(event_log)
    # else:
    #     print("Lifecycle available")
    #     # lifecycleavailable = True
    collapse_variants_flag = True
    if not xes_log_renamed:
        return jsonify({"error": "xes_log is required"}), 400

    time_granularity = TimeUnit.MS
    logging.info(xes_log)
    xes_log_renamed = to_interval(xes_log_renamed)
    # Perform variant analysis
    variants: dict[Group, list[Trace]] = get_concurrency_variants(xes_log_renamed, True, time_granularity)
    collapse_variants_list = collapse_variants(variants)

    if(collapse_variants_flag):
        collapse_variants_list = collapse_variants(variants)
    # Generate variant names and map activities
    variant_names = []

    # collapsed_variants = collapse_variants(variants)
    # Store in session and cache
    # session[VARIANTS_KEY] = variants
    # session[VARIANT_NAMES_KEY] = variant_names
    # session[LOG_KEY] = xes_log
    # save_to_cache(VARIANTS_KEY, variants)
    # save_to_cache(VARIANT_NAMES_KEY, variant_names)
    # save_to_cache(LOG_KEY, xes_log)
    return ({"variants": variants, "variant_names": variant_names, "collapse_variants_list": collapse_variants_list})
    # except Exception as e:
    #     return ({"error": str(e)}), 500
resutt = get_variants()

print(resutt['variants'])
print("\n------------------------------------------------------\n")
print(resutt['collapse_variants_list'])



def discover_process(variants):
    """
    API Endpoint: Discover Process
    ------------------------------
    Description:
        Discovers a process model from specified variants, filters logs for those variants,
        and uses PM4Py for process discovery.

    Request Payload:
        - variant_names (list): A list of variant names to use for discovery.

    Response:
        - process_model (dict): A discovered process model.

    Example Request:
        {
            "variant_names": ["Variant_1", "Variant_2"]
        }

    Example Response:
        {
            "process_model": {
                "nodes": ["Start", "A", "B", "C", "End"],
                "edges": [
                    {"from": "Start", "to": "A"},
                    {"from": "A", "to": "B"},
                    {"from": "B", "to": "C"},
                    {"from": "C", "to": "End"}
                ]
            }
        }
    """
    temp_filtered_log = []
    try:
        # for key, value in variants.items():
        #     key: Group  # Declare type for key
        #     value: list[Trace]
        #     temp_filtered_log = EventLog(value)

        print(temp_filtered_log)
        temp_filtered_log = get_traces_from_variants(variants)
        # Perform process discovery using PM4Py (Alpha Miner in this example)
        pt = discover_process_tree_inductive(temp_filtered_log, noise_threshold=0.1)
        apply_reduction_rules(pt)
        process_model, im, fm = pm4py.convert_to_petri_net(pt)
        pm4py.view_petri_net(process_model,im,fm)
        stringpnml = pnml.export_petri_as_string(process_model, im, fm)
        print("\nSNMLLL")
        print(stringpnml)
        # Store in session and cache
        # session[PROCESS_MODEL_KEY] = process_model
        # save_to_cache(PROCESS_MODEL_KEY, process_model)
        return ({"process_model": process_model})
    except Exception as e:
        print(str(e))
        return ({"error": str(e)}), 500

# logging.info(get_variants())
# print(resutt['variants'])
print("\n\nStarting discover")
model = discover_process(resutt['variants'])

def add_cvariants_to_process_model_unknown_conformance(selected_variants):
    selected_variants = get_traces_from_variants(selected_variants)

    fitting_traces = set()
    traces_to_add = set()
    process_tree, _ = dict_to_process_tree(d.pt)
    for selected_variant in selected_variants:
        if typed_trace_fits_process_tree(selected_variant, process_tree):
            fitting_traces.add(selected_variant)
        else:
            traces_to_add.add(selected_variant)

    return add_variants_to_process_model(
        pt,
        list(fitting_traces),
        list(traces_to_add),
        PoolFactory.instance().get_pool(),
    )

#
# print()