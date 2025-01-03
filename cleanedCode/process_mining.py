# services.py

import logging
import uuid
from typing import List, Dict, Any
from datetime import datetime
import statistics

import pm4py
from cortado_core.utils.cvariants import get_concurrency_variants
from cortado_core.utils.split_graph import Group
from cortado_core.utils.timestamp_utils import TimeUnit
from pm4py.objects.log.obj import EventLog, Trace
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments_algo
from pm4py.objects.log.util.interval_lifecycle import to_interval
from pm4py.objects.petri_net.exporter.exporter import pnml
from pm4py.util.xes_constants import DEFAULT_TIMESTAMP_KEY, DEFAULT_TRANSITION_KEY, DEFAULT_START_TIMESTAMP_KEY

from global_storage import GlobalStorage
from process_intelligence.collapse_variants import collapse_variants

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ProcessMiningService:
    """
    Encapsulates:
      - Import XES and generate named variants
      - Add custom variant
      - Discover model from variant subset
      - Fitness check
      - Create snapshot
    """

    def __init__(self, storage: GlobalStorage):
        self.storage = storage

    # ----------------------------------------------------------------
    # 1) Import log + Generate Named Variants
    # ----------------------------------------------------------------
    def generate_variants_from_xes(
        self,
        xes_file_path: str, log_id: str,
        collapse_flag: bool = True
    ) -> dict[str, dict[str, dict[str, Group | list[Trace] | int]]]:
        """
        Reads an XES log, optionally does concurrency merges or normal variants,
        assigns unique names, stores them in self.named_variants,
        and returns the dictionary.
        """
        # 1) Import the XES log
        log = xes_importer.apply(xes_file_path)
        logger.info(f"Imported XES log with {len(log)} traces from '{xes_file_path}'.")

        # 2) (Optional) rename attributes or convert to interval if needed
        if (DEFAULT_TRANSITION_KEY not in log[0][0]) and (DEFAULT_START_TIMESTAMP_KEY not in log[0][0]):
            log = to_interval(log)

        # 3) Extract concurrency-based variants (for demonstration)
        #    If you have a simpler approach, you can skip concurrency merges.
        time_granularity = TimeUnit.MS
        variants: Dict[Group, List[Trace]] = get_concurrency_variants(
            log,
            True,
            time_granularity=time_granularity
        )
        # print(variants)

        # Possibly collapse them
        named_collapsed_variants = {}

        if collapse_flag:
            collapsed_list = collapse_variants(variants)
            logger.info(f"Collapsed concurrency variants => got {len(collapsed_list)} items.")
            idx = 1
            for group, traces in collapsed_list.items():
                unique_id = uuid.uuid4().hex[:8]
                variant_dict = {}
                variant_name = f"Variant_{unique_id}"
                variant_dict['activities'] = group
                variant_dict['traces'] = traces
                variant_dict['no_of_traces'] = len(traces)

                named_collapsed_variants[variant_name] = variant_dict
                idx += 1

            # Save to self for future usage
            self.storage.save_named_collapsed_variants(log_id, named_collapsed_variants)
            logger.info(f"Assigned {len(named_collapsed_variants)} collapsed named variants from concurrency merges/collapses.")

        else:
            # Convert concurrency_data (dict) to a list of (Group, [Trace]) for uniformity
            collapsed_list = []

        # 4) Assign unique variant names
        named_variants = {}
        id=1
        for group, traces in variants.items():
            unique_id = uuid.uuid4().hex[:8]
            unique_id = id
            variant_dict = {}
            variant_name = f"Variant_{unique_id}"
            variant_dict['activities'] = group
            variant_dict['traces'] = traces
            variant_dict['no_of_traces'] = len(traces)
            id= id+1
            named_variants[variant_name] = variant_dict

        # Save to self for future usage
        self.storage.save_named_variants(log_id, named_variants)
        logger.info(f"Assigned {len(named_variants)} named variants.")

        return {'named_variants' : named_variants, 'named_collapsed_variants': named_collapsed_variants}
    # ----------------------------------------------------------------
    # 2) Add a Custom Variant
    # ----------------------------------------------------------------
    def add_custom_variant(self, log_id: str, variant_name: str, activity_sequence: List[str]):
        """
        - Retrieve existing named variants for log_id
        - Build a pm4py Trace from activity_sequence
        - Append (or create) that trace under 'variant_name'
        """
        all_variants = self.storage.get_named_variants(log_id)
        if not all_variants:
            raise ValueError(f"No variants found for log_id '{log_id}'. Generate them first.")

        # Build a new trace
        new_trace = Trace()
        for act in activity_sequence:
            new_trace.append({"concept:name": act})

        if variant_name in all_variants:
            all_variants[variant_name].append(new_trace)
        else:
            all_variants[variant_name] = [new_trace]

        self.storage.save_variants(log_id, all_variants)
        logger.info(f"Added custom variant '{variant_name}' to log '{log_id}' with {len(activity_sequence)} events")

    # ----------------------------------------------------------------
    # 3) Discover a Petri net from chosen variants
    # ----------------------------------------------------------------
    def discover_process_from_variants(self, log_id: str, variant_names: List[str], model_id: str) -> Dict[str, Any]:
        """
        - Merge the specified named variants into a single EventLog
        - Inductive Miner -> Petri net
        - Alignments for fitness
        - Export PNML
        - Save under models[model_id]
        Returns the model_data dict
        """
        all_variants = self.storage.get_named_variants(log_id)
        if not all_variants:
            raise ValueError(f"No variants found for log_id '{log_id}'")

        combined_traces = []
        for vname in variant_names:
            if vname not in all_variants:
                raise ValueError(f"Variant '{vname}' not found in log '{log_id}'")
            combined_traces.extend(all_variants[vname]['traces'])

        if not combined_traces:
            raise ValueError("No traces found for the chosen variants")

        combined_log = EventLog(combined_traces)

        # Discover Petri net via Inductive Miner
        process_tree = inductive_miner.apply(combined_log)

        net, im, fm = pm4py.convert_to_petri_net(process_tree)

        pm4py.view_petri_net(net, im, fm)
        # Alignment-based fitness
        alignment_params = {
            alignments_algo.Variants.VERSION_STATE_EQUATION_A_STAR.value.Parameters.PARAM_ALIGNMENT_RESULT_IS_SYNC_PROD_AWARE: True
        }
        alignment_results = alignments_algo.apply_log(combined_log, net, im, fm, parameters=alignment_params)
        total_fit = 0.0
        for ares in alignment_results:
            total_fit += ares.get("fitness", 1.0)
        avg_fitness = total_fit / len(alignment_results) if alignment_results else 1.0

        # Export PNML
        pnml_string = pnml.export_petri_as_string(net, im, fm)

        # Placeholder for extra metrics (soundness, precision, etc.)
        model_metrics = {
            "fitness": avg_fitness,
            "soundness": 1.0,       # example placeholder
            "precision": 0.9,      # example placeholder
            "generalization": 0.9  # example placeholder
        }

        model_data = {
            "net": net,
            "im": im,
            "fm": fm,
            "pnml": pnml_string,
            "metrics": model_metrics,
            "variants_used": variant_names
        }

        self.storage.save_model(model_id, model_data)
        logger.info(f"Discovered model '{model_id}' (fitness={avg_fitness:.3f}), from variants={variant_names}")
        return model_data

    # ----------------------------------------------------------------
    # 4) Fitness Check Only
    # ----------------------------------------------------------------
    def check_fitness_only(self, model_id: str, log_id: str, variant_names: List[str]) -> Dict[str, Any]:
        """
        - Retrieve discovered model
        - Merge given variant traces
        - Alignment-based conformance
        Returns average_fitness, alignment_count, etc.
        """
        model_data = self.storage.get_model(model_id)
        if not model_data:
            raise ValueError(f"Model '{model_id}' not found in storage.")

        net = model_data["net"]
        im = model_data["im"]
        fm = model_data["fm"]

        all_variants = self.storage.get_named_variants(log_id)
        if not all_variants:
            raise ValueError(f"No variants found for log_id '{log_id}'")

        combined_traces = []
        for vname in variant_names:
            if vname not in all_variants:
                raise ValueError(f"Variant '{vname}' not found in log '{log_id}'")
            combined_traces.extend(all_variants[vname]['traces'])

        if not combined_traces:
            raise ValueError("No traces for fitness check.")

        combined_log = EventLog(combined_traces)
        alignments = alignments_algo.apply_log(combined_log, net, im, fm)
        total_fit = 0.0
        for al in alignments:
            total_fit += al.get("fitness", 0.0)
        avg_fitness = total_fit / len(alignments) if alignments else 1.0

        return {
            "model_id": model_id,
            "variants_checked": variant_names,
            "average_fitness": avg_fitness,
            "alignment_count": len(alignments),
        }

    # ----------------------------------------------------------------
    # 5) Create Snapshot (Custom JSON structure)
    # ----------------------------------------------------------------

    def create_snapshot(self, snapshot_id: str, model_id: str, log_id: str, variant_names: List[str]) -> Dict[str, Any]:
        """
        Main entry point for creating a snapshot.
        Returns a final snapshot JSON dictionary and stores it in self.storage.
        """
        model_data = self.storage.get_model(model_id)
        if not model_data:
            raise ValueError(f"Model '{model_id}' not found.")

        # 1) Retrieve net, im, fm for alignment
        net = model_data["net"]
        im = model_data["im"]
        fm = model_data["fm"]

        # 2) Retrieve the named variants from storage
        all_variants = self.storage.get_named_variants(log_id)
        if not all_variants:
            raise ValueError(f"No variants found for log '{log_id}'")

        # Filter only the user-specified variants
        relevant_variants = {
            vname: all_variants[vname]
            for vname in variant_names
            if vname in all_variants
        }

        # 3) Build detailed data for each variant
        variant_details = []
        snapshot_level_durations = []
        snapshot_level_fitness = []
        snapshot_level_anomalies = 0
        total_journeys = 0

        for variant_name, value_dictionary in relevant_variants.items():
            variant_info = self._compute_variant_metrics(variant_name, value_dictionary['traces'], net, im, fm)
            variant_details.append(variant_info)

            # Collect data for snapshot-level aggregation
            snapshot_level_durations.extend(variant_info["__durations"])  # hidden field for aggregation
            snapshot_level_fitness.extend(variant_info["__fitness_list"])
            snapshot_level_anomalies += variant_info["variant_metrics"]["anomalies_count"]
            total_journeys += variant_info["variant_metrics"]["frequency"]

        # 4) Compute snapshot-level aggregated metrics
        snapshot_metrics = self._compute_snapshot_metrics(
            durations=snapshot_level_durations,
            fitness_list=snapshot_level_fitness,
            total_variants=len(variant_details),
            total_journeys=total_journeys,
            anomalies_count=snapshot_level_anomalies,
            model_fitness=model_data["metrics"]["fitness"]
        )

        # 5) Build final snapshot JSON
        snapshot_json = self._build_snapshot_json(
            snapshot_id, model_id, model_data, variant_details, snapshot_metrics
        )

        # 6) Save in storage
        self.storage.save_snapshot(snapshot_id, {"snapshot": snapshot_json})
        logger.info(f"Snapshot '{snapshot_id}' created with {total_journeys} journeys across {len(variant_details)} variants.")
        return {"snapshot": snapshot_json}

    # ------------------------------------------------------------------
    # A) For each variant, compute metrics + journeys
    # ------------------------------------------------------------------
    def _compute_variant_metrics(
        self,
        variant_name: str,
        traces: List[Trace],
        net, im, fm
    ) -> Dict[str, Any]:
        """
        Align all traces in this variant, compute durations, anomalies, per-journey data,
        then produce aggregated metrics (min, max, average, median).
        """
        variant_log = EventLog(traces)

        # 1) Align the entire variant log
        align_params = {
            alignments_algo.Variants.VERSION_STATE_EQUATION_A_STAR.value.Parameters.PARAM_ALIGNMENT_RESULT_IS_SYNC_PROD_AWARE: True
        }
        alignment_results = alignments_algo.apply_log(variant_log, net, im, fm, parameters=align_params)

        # 2) Build journey-level data
        journeys = []
        durations = []
        fitness_list = []
        anomalies_count = 0

        for idx, trace in enumerate(traces):
            alignment = alignment_results[idx]
            trace_fitness = alignment.get("fitness", 1.0)

            # Compute duration if timestamps exist
            duration_seconds = self._compute_trace_duration(trace)

            # Anomalies if trace_fitness < 1, or log_moves/model_moves exist
            log_moves = alignment.get("log_moves", [])
            model_moves = alignment.get("model_moves", [])
            is_anomalous = (trace_fitness < 1.0 or len(log_moves) > 0 or len(model_moves) > 0)
            anomalies_list = []
            if is_anomalous:
                anomalies_count += 1
                # optional: store details for each move
                for lm in log_moves:
                    anomalies_list.append({
                        "anomaly_id": f"lm_{idx}",
                        "type": "Log Move",
                        "details": str(lm)
                    })
                for mm in model_moves:
                    anomalies_list.append({
                        "anomaly_id": f"mm_{idx}",
                        "type": "Model Move",
                        "details": str(mm)
                    })

            # Build a journey ID
            journey_id = f"journey_{variant_name}_{idx+1}"
            status = "Completed" if trace_fitness == 1.0 else "Conformance Issues"
            event_sequence = [evt.get("concept:name", "Unknown") for evt in trace]

            # Gather journey info
            journeys.append({
                "journey_id": journey_id,
                "start_time": self._get_trace_start_time(trace),
                "end_time": self._get_trace_end_time(trace),
                "status": status,
                "event_sequence": event_sequence,
                "journey_metrics": {
                    "duration": duration_seconds,
                    "bottlenecks": [],
                    "fitness_score": trace_fitness,
                    "anomalies": anomalies_list
                }
            })

            durations.append(duration_seconds)
            fitness_list.append(trace_fitness)

        # 3) Aggregate metrics at variant level
        freq = len(traces)
        if freq > 0:
            avg_duration = statistics.mean(durations)
            med_duration = statistics.median(durations)
            min_duration = min(durations)
            max_duration = max(durations)
        else:
            avg_duration = med_duration = min_duration = max_duration = 0.0

        # Build the final variant info dictionary
        variant_info = {
            "variant_id": variant_name,
            "variant_name": variant_name,
            "activity_sequence": self._get_activity_sequence(traces),
            "variant_metrics": {
                "frequency": freq,
                "average_duration": avg_duration,
                "median_duration": med_duration,
                "min_duration": min_duration,
                "max_duration": max_duration,
                "confirmation_level": 1.0,
                "total_frequency": freq,
                "global_average_duration": avg_duration,
                "anomalies_count": anomalies_count
            },
            "journeys": journeys,
            # Extra fields for snapshot-level aggregation (we'll remove them later)
            "__durations": durations,
            "__fitness_list": fitness_list
        }
        return variant_info

    # ------------------------------------------------------------------
    # B) Compute snapshot-level metrics from aggregated data
    # ------------------------------------------------------------------
    def _compute_snapshot_metrics(
        self,
        durations: List[float],
        fitness_list: List[float],
        total_variants: int,
        total_journeys: int,
        anomalies_count: int,
        model_fitness: float
    ) -> Dict[str, Any]:
        """
        Summarizes data across all chosen variants: durations, fitness, anomalies, etc.
        """
        if durations:
            avg_duration = statistics.mean(durations)
            median_duration = statistics.median(durations)
        else:
            avg_duration = 0.0
            median_duration = 0.0

        if fitness_list:
            avg_fitness = statistics.mean(fitness_list)
        else:
            avg_fitness = model_fitness

        snapshot_metrics = {
            "total_variants": total_variants,
            "total_journeys": total_journeys,
            "average_duration": avg_duration,
            "median_duration": median_duration,
            "anomalies_count": anomalies_count,
            "average_fitness_score": avg_fitness
        }
        return snapshot_metrics

    # ------------------------------------------------------------------
    # C) Build the final snapshot JSON structure
    # ------------------------------------------------------------------
    def _build_snapshot_json(
        self,
        snapshot_id: str,
        model_id: str,
        model_data: Dict[str, Any],
        variant_details: List[Dict[str, Any]],
        snapshot_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Remove extra fields from variant_details, then construct final snapshot dict.
        """
        # Remove the "__durations" and "__fitness_list" from each variant
        for vinfo in variant_details:
            if "__durations" in vinfo:
                del vinfo["__durations"]
            if "__fitness_list" in vinfo:
                del vinfo["__fitness_list"]

        # Build the final snapshot JSON
        now_str = datetime.now().isoformat() + "Z"
        return {
            "snapshot_id": snapshot_id,
            "timestamp": now_str,
            "model": {
                "model_id": model_id,
                "model_name": "Discovered Petri Net",
                "model_type": "Petri Net",
                "model_definition": model_data["pnml"],
                "model_metrics": {
                    "fitness": model_data["metrics"].get("fitness", 1.0),
                    "soundness": model_data["metrics"].get("soundness", 1.0),
                    "precision": model_data["metrics"].get("precision", 0.9),
                    "generalization": model_data["metrics"].get("generalization", 0.9)
                }
            },
            "variants": variant_details,
            "snapshot_metrics": snapshot_metrics
        }

    # ------------------------------------------------------------------
    # Utility: get start/end times, compute durations, etc.
    # ------------------------------------------------------------------
    def _get_trace_start_time(self, trace: Trace) -> str:
        if len(trace) == 0:
            return None
        start_dt = trace[0].get(DEFAULT_TIMESTAMP_KEY)
        return start_dt.isoformat() if start_dt else None

    def _get_trace_end_time(self, trace: Trace) -> str:
        if len(trace) == 0:
            return None
        end_dt = trace[-1].get(DEFAULT_TIMESTAMP_KEY)
        return end_dt.isoformat() if end_dt else None

    def _compute_trace_duration(self, trace: Trace) -> float:
        if len(trace) < 2:
            return 0.0
        start_dt = trace[0].get(DEFAULT_TIMESTAMP_KEY)
        end_dt = trace[-1].get(DEFAULT_TIMESTAMP_KEY)
        if start_dt and end_dt:
            return (end_dt - start_dt).total_seconds()
        return 0.0

    def _get_activity_sequence(self, traces: List[Trace]) -> List[str]:
        """
        Just fetch activity sequence from the first trace if available.
        """
        if not traces:
            return []
        first_trace = traces[0]
        return [evt.get("concept:name", "Unknown") for evt in first_trace]
