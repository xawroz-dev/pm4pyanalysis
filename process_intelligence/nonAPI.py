import logging
from collections import defaultdict

import pm4py
from cortado_core.utils.collapse_variants import collapse_variants
from flask import Flask, request, jsonify, session
from cortado_core.utils.cvariants import get_detailed_variants, get_concurrency_variants
from cortado_core.utils.split_graph import Group
from cortado_core.utils.timestamp_utils import TimeUnit
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.objects.log.obj import EventLog, Trace
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import xes as xes_utils

from pm4py.discovery import discover_process_tree_inductive
#     variant_analysis,
#     process_discovery,
#     model_adjustment,
#     conformance_check,
# )
from flask_caching import Cache
from cortado_core.process_tree_utils.reduction import apply_reduction_rules

from multithreading.pool_factory import PoolFactory
from utils import get_traces_from_variants

# Initialize Flask app and caching
app = Flask(__name__)
app.secret_key = "supersecretkey"  # Required for session management
cache = Cache(app, config={"CACHE_TYPE": "SimpleCache"})

# Cache keys
VARIANTS_KEY = "cached_variants"
PROCESS_MODEL_KEY = "cached_process_model"
VARIANT_NAMES_KEY = "variant_names"
LOG_KEY = "xes_log"


# Helper functions
def save_to_cache(key, value):
    cache.set(key, value)


def get_from_cache(key):
    return cache.get(key)


def clear_cache():
    cache.clear()
    session.pop(VARIANTS_KEY, None)
    session.pop(PROCESS_MODEL_KEY, None)
    session.pop(VARIANT_NAMES_KEY, None)
    session.pop(LOG_KEY, None)

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
    try:
        # data = request.get_json()
        xes_log = xes_importer.apply("/Users/sashrestha/PycharmProjects/process_intelligence/log.xes")
        collapse_variants_flag = False
        if not xes_log:
            return jsonify({"error": "xes_log is required"}), 400

        time_granularity = TimeUnit.MS
        # Perform variant analysis
        variants: dict[Group, list[Trace]] = get_concurrency_variants(xes_log, True, time_granularity, PoolFactory.instance().get_pool())
        collapse_variants_list = collapse_variants(variants)

        if(collapse_variants_flag):
            collapse_variants_list = collapse_variants(variants)
        # Generate variant names and map activities
        variant_names = generate_variant_names(variants)

        # Store in session and cache
        # session[VARIANTS_KEY] = variants
        # session[VARIANT_NAMES_KEY] = variant_names
        # session[LOG_KEY] = xes_log
        # save_to_cache(VARIANTS_KEY, variants)
        # save_to_cache(VARIANT_NAMES_KEY, variant_names)
        # save_to_cache(LOG_KEY, xes_log)
        return jsonify({"variants": variants, "variant_names": variant_names, "collapse_variants_list": collapse_variants_list})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

print(get_variants())
logging.info(get_variants())

@app.route("/api/discover_process", methods=["POST"])
def discover_process():
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
    try:
        data = request.get_json()
        variant_names = data.get("variant_names")
        if not variant_names:
            return jsonify({"error": "variant_names is required"}), 400

        # Retrieve cached log and variant names
        xes_log = get_from_cache(LOG_KEY) or session.get(LOG_KEY)
        variant_mapping = get_from_cache(VARIANT_NAMES_KEY) or session.get(VARIANT_NAMES_KEY)

        if not xes_log or not variant_mapping:
            return jsonify({"error": "No cached log or variants found. Call /api/variants first."}), 400

        # Import the log using PM4Py
        log = xes_importer.apply(xes_log)

        # Filter log based on the specified variant names
        filtered_log = None
        for variant_name in variant_names:
            if variant_name not in variant_mapping:
                return jsonify({"error": f"Variant '{variant_name}' not found."}), 400

            variant_events = variant_mapping[variant_name]
            temp_filtered_log = variants_filter.filter_log_variants_by_activity(
                log, variant_events
            )

            # Merge the filtered logs for each variant
            if filtered_log is None:
                filtered_log = temp_filtered_log
            else:
                filtered_log.extend(temp_filtered_log)
        temp_filtered_log = get_traces_from_variants(variant_names)
        # Perform process discovery using PM4Py (Alpha Miner in this example)
        pt = discover_process_tree_inductive(temp_filtered_log, noise_threshold=0.1)
        apply_reduction_rules(pt)
        process_model = pm4py.convert_to_petri_net(pt)
        # Store in session and cache
        session[PROCESS_MODEL_KEY] = process_model
        save_to_cache(PROCESS_MODEL_KEY, process_model)
        return jsonify({"process_model": process_model})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/adjust_model", methods=["POST"])
def adjust_model():
    """
    API Endpoint: Adjust Process Model
    ----------------------------------
    Description:
        Adjusts a given process model to fit new events.

    Request Payload:
        - current_model (dict): The existing process model.
        - new_events (list): A list of new events to fit into the model.

    Response:
        - adjusted_model (dict): The adjusted process model.

    Example Request:
        {
            "current_model": {
                "nodes": ["Start", "A", "B", "C", "End"],
                "edges": [
                    {"from": "Start", "to": "A"},
                    {"from": "A", "to": "B"},
                    {"from": "B", "to": "C"},
                    {"from": "C", "to": "End"}
                ]
            },
            "new_events": ["A", "D", "End"]
        }

    Example Response:
        {
            "adjusted_model": {
                "nodes": ["Start", "A", "B", "C", "D", "End"],
                "edges": [
                    {"from": "Start", "to": "A"},
                    {"from": "A", "to": "B"},
                    {"from": "B", "to": "C"},
                    {"from": "C", "to": "End"},
                    {"from": "A", "to": "D"},
                    {"from": "D", "to": "End"}
                ]
            }
        }
    """
    try:
        data = request.get_json()
        current_model = data.get("current_model")
        new_events = data.get("new_events")
        if not current_model or not new_events:
            return jsonify(
                {"error": "current_model and new_events are required"}
            ), 400
        all_traces = get_traces_from_variants(data.vaiants)

        pt = discover_process_tree_inductive(all_traces, noise_threshold=0.1)
        apply_reduction_rules(pt)
        process_model = pm4py.convert_to_petri_net(pt)
        return jsonify({"adjusted_model": process_model})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs_by_variant", methods=["POST"])
def logs_by_variant():
    """
    API Endpoint: Logs by Variant
    -----------------------------
    Description:
        Takes a list of variant names and returns the logs associated with those variants.

    Request Payload:
        - variant_names (list): A list of variant names to fetch the logs for.

    Response:
        - grouped_logs (dict): A dictionary where keys are the variant names, and values are lists of logs for those variants.

    Example Request:
        {
            "variant_names": ["Variant_1", "Variant_2"]
        }

    Example Response:
        {
            "grouped_logs": {
                "Variant_1": ["log1", "log2"],
                "Variant_2": ["log3"]
            }
        }
    """
    try:
        data = request.get_json()
        variant_names = data.get("variant_names")
        if not variant_names:
            return jsonify({"error": "variant_names is required"}), 400

        # Retrieve cached log and variant names
        xes_log = get_from_cache(LOG_KEY) or session.get(LOG_KEY)
        variant_mapping = get_from_cache(VARIANT_NAMES_KEY) or session.get(VARIANT_NAMES_KEY)
        variants_dict = get_variants(xes_log)

        if not xes_log or not variant_mapping:
            return jsonify({"error": "No cached log or variants found. Call /api/variants first."}), 400


        grouped_logs = {}
        for variant_name in variant_names:
            if variant_name not in variant_mapping:
                return jsonify({"error": f"Variant '{variant_name}' not found."}), 400

            # Get all variants

            # Retrieve traces for the target variant

            # Convert filtered log to a human-readable format (e.g., list of case IDs or event sequences)
            grouped_logs[variant_name] = variants_dict.get(variant_name, [])

        return jsonify({"grouped_logs": grouped_logs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
