#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cortado-Inspired Pipeline with Iterative Repair
-----------------------------------------------
1) Build or load a PM4Py log (e.g., from CSV, XES, etc.).
2) Extract variants; build partial orders merging parallel/cyclic tasks (Cortado style).
3) Merge partial orders across variants -> Petri net.
4) Perform conformance check (token replay).
5) Iterative repair if fitness < threshold:
    - Identify missing activities
    - Repair the net by adding them
    - Re-check fitness
    - Continue until either fitness >= threshold or no more missing acts
6) Create a snapshot with final net, conformance, iteration logs, etc.

Author: ChatGPT (2024)
License: MIT or your chosen license
"""

import os
import json
import logging
from datetime import datetime
from collections import defaultdict

import pm4py
from pm4py.objects.log.log import EventLog, Trace, Event
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
from pm4py.objects.petri.petrinet import PetriNet, Marking
from pm4py.objects.petri.utils import petri_utils


# ---------------------------
# CONFIG
# ---------------------------
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("CortadoIterativeRepair")

OUTPUT_FOLDER = "snapshots_iterative_repair"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

FITNESS_THRESHOLD = 0.8
MAX_REPAIR_ATTEMPTS = 5  # to avoid infinite loops if fitness never improves


# -----------------------------------------------------------------------
# 1. EXAMPLE: Generate or Load a Log
# -----------------------------------------------------------------------
def load_your_log() -> EventLog:
    """
    Here you can load a PM4Py EventLog. For demonstration, we create
    a trivial log or you can replace this with a real importer.
    """
    # Example: Trivial synthetic log
    log = EventLog()
    # ... populate the log ...
    # OR call pm4py.objects.log.importer.xes.importer.apply(xes_path)
    return log


# -----------------------------------------------------------------------
# 2. Extract Variants & Build Partial Orders
#    (Cortado-like concurrency/cycle merges)
# -----------------------------------------------------------------------
def extract_variants(log: EventLog):
    variants_dict = variants_filter.get_variants(log)
    logger.info(f"Extracted {len(variants_dict)} variant(s).")
    return variants_dict

def build_cortado_partial_orders(variants_dict):
    """
    For each variant, create a naive partial order that merges parallel/cyclic tasks
    by removing edges that cause loops (concurrency).
    Returns list of (po, node_labels).
    """
    partial_orders = []
    for v_name, traces in variants_dict.items():
        if not traces:
            continue
        trace = traces[0]
        po, node_labels = linear_partial_order(trace)
        unify_parallel_cyclic_in_variant(po, node_labels)
        partial_orders.append((po, node_labels))
    return partial_orders

def linear_partial_order(trace: Trace):
    """Direct-follows adjacency: node_0->node_1->..."""
    po = {}
    node_labels = {}
    for i, evt in enumerate(trace):
        node_id = f"n_{i}"
        po[node_id] = set()
        node_labels[node_id] = evt["concept:name"]
    for i in range(len(trace) - 1):
        po[f"n_{i}"].add(f"n_{i+1}")
    return po, node_labels

def unify_parallel_cyclic_in_variant(po, labels):
    edges_to_remove = []
    for s in list(po.keys()):
        for t in list(po[s]):
            if introduces_cycle(po, s, t):
                edges_to_remove.append((s, t))
    for (s, t) in edges_to_remove:
        po[s].remove(t)

def introduces_cycle(po, src, tgt):
    visited = set()
    def dfs(n):
        if n == src:
            return True
        visited.add(n)
        for sc in po[n]:
            if sc not in visited:
                if dfs(sc):
                    return True
        return False
    return dfs(tgt)


# -----------------------------------------------------------------------
# 3. Merge Partial Orders
# -----------------------------------------------------------------------
def merge_partial_orders(po_list):
    """
    Merge partial orders from multiple variants, unifying tasks with same labels,
    removing edges that cause loops => concurrency.
    Returns merged_po, merged_labels
    """
    from collections import defaultdict
    merged_po = defaultdict(set)
    merged_labels = {}
    global_id = 0

    for (po, labels) in po_list:
        local_to_global = {}
        for node, act_label in labels.items():
            # unify with existing global node if label matches
            candidate = None
            for gn, glabel in merged_labels.items():
                if glabel == act_label:
                    candidate = gn
                    break
            if candidate is None:
                gnode = f"g_{global_id}"
                global_id += 1
                merged_po[gnode] = set()
                merged_labels[gnode] = act_label
                local_to_global[node] = gnode
            else:
                local_to_global[node] = candidate
        # unify edges
        for s, succs in po.items():
            gs = local_to_global[s]
            for t in succs:
                gt = local_to_global[t]
                if gs != gt:
                    merged_po[gs].add(gt)

    # remove loops
    remove_cycles_in_merged_po(merged_po)
    return dict(merged_po), merged_labels

def remove_cycles_in_merged_po(po):
    edges_to_remove = []
    for s in list(po.keys()):
        for t in list(po[s]):
            if introduces_cycle(po, s, t):
                edges_to_remove.append((s, t))
    for (s, t) in edges_to_remove:
        po[s].remove(t)


# -----------------------------------------------------------------------
# 4. Build Petri Net from Merged PO
# -----------------------------------------------------------------------
def partial_order_to_petrinet(merged_po, merged_labels):
    net = PetriNet("IterativeRepairNet")
    transitions_map = {}
    # transitions
    for node, label in merged_labels.items():
        t = PetriNet.Transition(node, label=label)
        net.transitions.add(t)
        transitions_map[node] = t
    # places
    for s, scs in merged_po.items():
        for t in scs:
            p_name = f"p_{s}_to_{t}"
            p = PetriNet.Place(p_name)
            net.places.add(p)
            petri_utils.add_arc_from_to(transitions_map[s], p, net)
            petri_utils.add_arc_from_to(p, transitions_map[t], net)

    im = Marking()
    fm = Marking()

    def incoming(n):
        inc = []
        for x, succs in merged_po.items():
            if n in succs:
                inc.append(x)
        return inc

    for node in merged_po.keys():
        if len(incoming(node)) == 0:
            p_init = PetriNet.Place(f"init_{node}")
            net.places.add(p_init)
            petri_utils.add_arc_from_to(p_init, transitions_map[node], net)
            im[p_init] = 1
        if len(merged_po[node]) == 0:
            p_final = PetriNet.Place(f"final_{node}")
            net.places.add(p_final)
            petri_utils.add_arc_from_to(transitions_map[node], p_final, net)
            fm[p_final] = 1
    logger.info("Petri net built.")
    return net, im, fm


# -----------------------------------------------------------------------
# 5. Conformance & Iterative Repair
# -----------------------------------------------------------------------
def perform_conformance(log: EventLog, net: PetriNet, im: Marking, fm: Marking):
    logger.info("Performing conformance check (token replay)...")
    fitness_dict = token_replay.apply(log, net, im, fm)
    trace_results = token_replay.apply(log, net, im, fm, variant="non_missing_toks")
    logger.info(f"Token replay fitness => {fitness_dict}")
    return fitness_dict, trace_results

def identify_missing_activities(log: EventLog, net: PetriNet):
    log_acts = set()
    for trace in log:
        for evt in trace:
            log_acts.add(evt["concept:name"])
    net_acts = set(t.label for t in net.transitions if t.label is not None)
    return log_acts - net_acts

def repair_net_minimal(net: PetriNet, im: Marking, fm: Marking, missing_acts):
    """
    Naive approach: each missing activity => transition + two places.
    """
    if not missing_acts:
        return net, im, fm
    logger.warning(f"Repairing net: adding {len(missing_acts)} missing activities.")
    for act in missing_acts:
        t_name = f"r_{act}"
        t_new = PetriNet.Transition(t_name, label=act)
        net.transitions.add(t_new)
        p_in = PetriNet.Place(f"pin_{act}")
        p_out = PetriNet.Place(f"pout_{act}")
        net.places.add(p_in)
        net.places.add(p_out)
        petri_utils.add_arc_from_to(p_in, t_new, net)
        petri_utils.add_arc_from_to(t_new, p_out, net)
    return net, im, fm


def iterative_repair_loop(log: EventLog, net: PetriNet, im: Marking, fm: Marking,
                          threshold=FITNESS_THRESHOLD, max_attempts=MAX_REPAIR_ATTEMPTS):
    """
    1) Conformance check
    2) If fitness < threshold, identify missing acts & repair
    3) Re-check fitness
    4) Repeat until fitness >= threshold or no new missing acts or attempts exhausted
    Returns (final_net, final_im, final_fm, final_fitness, conformance_results).
    """
    attempt = 0
    while True:
        attempt += 1
        fitness_dict, trace_results = perform_conformance(log, net, im, fm)
        curr_fitness = fitness_dict.get("log_fitness", 0.0)
        logger.info(f"[Iteration {attempt}] Current fitness: {curr_fitness}")

        if curr_fitness >= threshold:
            logger.info(f"Fitness {curr_fitness} >= threshold {threshold}. Stopping repair.")
            return net, im, fm, fitness_dict, trace_results

        if attempt >= max_attempts:
            logger.warning(f"Reached max repair attempts ({max_attempts}). Stopping.")
            return net, im, fm, fitness_dict, trace_results

        # identify missing
        missing = identify_missing_activities(log, net)
        if not missing:
            logger.warning("No missing activities found, but fitness still below threshold. Stopping.")
            return net, im, fm, fitness_dict, trace_results

        # do repair
        net, im, fm = repair_net_minimal(net, im, fm, missing)
        # loop continues for next iteration


# -----------------------------------------------------------------------
# 6. CREATE SNAPSHOT
# -----------------------------------------------------------------------
def create_snapshot(
        snapshot_id,
        net,
        im,
        fm,
        fitness,
        trace_results,
        variant_analysis,
        iteration_notes=None
):
    logger.info(f"Creating snapshot {snapshot_id}...")
    from pm4py.objects.petri.exporter import exporter as petri_exporter

    try:
        net_str = pm4py.objects.petri.exporter.to_string(net)
    except Exception as e:
        net_str = f"Could not export net: {e}"

    snapshot = {
        "snapshot_id": snapshot_id,
        "timestamp": datetime.now().isoformat(),
        "model": {
            "petri_net_definition": net_str,
            "initial_marking": str(im),
            "final_marking": str(fm)
        },
        "analysis_results": {
            "fitness": fitness,
            "variant_analysis": variant_analysis,
            "conformance_results": [
                {
                    "trace_is_fit": c.get("trace_is_fit", None),
                    "enabled_transitions_in_example_of_model": c.get("enabled_transitions_in_example_of_model", [])
                } for c in trace_results
            ]
        }
    }

    if iteration_notes:
        snapshot["analysis_results"]["iteration_notes"] = iteration_notes

    out_file = os.path.join(OUTPUT_FOLDER, f"{snapshot_id}.json")
    with open(out_file, "w") as f:
        json.dump(snapshot, f, indent=4)

    logger.info(f"Snapshot saved -> {out_file}")
    return snapshot


# -----------------------------------------------------------------------
# 7. MAIN PIPELINE
# -----------------------------------------------------------------------
def cortado_iterative_repair_pipeline(log: EventLog, threshold=FITNESS_THRESHOLD):
    """
    1) Extract variants -> partial orders
    2) Merge -> build net
    3) Iterative repair loop if fitness < threshold
    4) Create snapshot
    """
    # extract variants
    variants_dict = extract_variants(log)
    variant_analysis = [
        {"variant_name": v_name, "frequency": len(traces)}
        for v_name, traces in variants_dict.items()
    ]

    # build partial orders
    partial_orders = build_cortado_partial_orders(variants_dict)

    # merge partial orders
    merged_po, merged_labels = merge_partial_orders(partial_orders)

    # petri net
    net, im, fm = partial_order_to_petrinet(merged_po, merged_labels)

    # iterative repair
    net, im, fm, final_fitness, conformance_results = iterative_repair_loop(
        log, net, im, fm, threshold
    )

    # create snapshot
    snapshot_id = f"iterative_repair_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    snapshot = create_snapshot(
        snapshot_id=snapshot_id,
        net=net,
        im=im,
        fm=fm,
        fitness=final_fitness,
        trace_results=conformance_results,
        variant_analysis=variant_analysis,
        iteration_notes="Multiple attempts to repair until fitness threshold is met or no more missing activities."
    )

    return snapshot


# -----------------------------------------------------------------------
# DEMO
# -----------------------------------------------------------------------
if __name__ == "__main__":
    """
    Example usage:
      - Load or create a log
      - Run the pipeline
      - Check the final snapshot
    """
    # 1) Load your log (replace with actual logic)
    log = load_your_log()
    # 2) Run pipeline
    final_snapshot = cortado_iterative_repair_pipeline(log, threshold=0.8)
    logger.info("Final snapshot content:")
    logger.info(json.dumps(final_snapshot, indent=4))
