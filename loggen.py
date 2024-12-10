import random
import datetime
import uuid
from pm4py.objects.log.obj import EventLog, Trace, Event
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
from pm4py.algo.evaluation.replay_fitness.variants import alignment_based as alignment_eval
from pm4py.util.xes_constants import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY


def generate_synthetic_log(num_traces=50):
    """
    Generate a synthetic event log that simulates a credit card application process with:
    - Parallel checks: Credit score checks and AML/Fraud checks in parallel with address validation.
    - Cyclic execution: Multiple address validation attempts until requirements are met.
    - Asynchronous calls: AML and Fraud checks represented as start/end events to simulate async behavior.
    - Error boundaries: If the final decision check fails, we do a manual review.

    Process outline (not a strict BPMN, but conceptual):

    START -> CUSTOMER_ID_VALIDATION -> [PARALLEL: {CREDIT_SCORE_CHECK, AML/FRAUD checks (async) -> CREDIT_CHECK_COMPLETE}
    + {ADDRESS_VALIDATION_CHECK loop until verified -> ADDRESS_VALIDATION_COMPLETE}]
    -> JOIN -> FINAL_DECISION_CHECK -> (Either CARD_ISSUED or MANUAL_REVIEW)

    Activities:
    - CUSTOMER_ID_VALIDATION
    - CREDIT_SCORE_CHECK (A1)
    - AML_CHECK_ASYNC_START / AML_CHECK_ASYNC_END (A2 async)
    - FRAUD_CHECK_ASYNC_START / FRAUD_CHECK_ASYNC_END (A3 async)
    - CREDIT_CHECK_COMPLETE (A_COMPLETE)
    - ADDRESS_VALIDATION_CHECK (B_CHECK_LOOP)
    - ADDRESS_VALIDATION_RETRY (B_TASK_LOOP - repeated 0..3 times)
    - ADDRESS_VALIDATION_COMPLETE (B_COMPLETE)
    - FINAL_DECISION_CHECK (may lead to MANUAL_REVIEW or CARD_ISSUED)
    - MANUAL_REVIEW (error handling)
    - CARD_ISSUED (completion)
    """

    log = EventLog()
    base_time = datetime.datetime.now()

    for _ in range(num_traces):
        trace = Trace()
        trace_id = str(uuid.uuid4())
        trace.attributes["concept:name"] = trace_id

        current_time = base_time
        # START
        trace.append(Event({DEFAULT_NAME_KEY: "START", DEFAULT_TIMESTAMP_KEY: current_time}))

        # CUSTOMER_ID_VALIDATION
        current_time += datetime.timedelta(seconds=random.randint(1, 5))
        trace.append(Event({DEFAULT_NAME_KEY: "CUSTOMER_ID_VALIDATION", DEFAULT_TIMESTAMP_KEY: current_time}))

        # Parallel simulation start:
        # Branch A: Credit checks + AML/Fraud async checks
        a_start = current_time + datetime.timedelta(seconds=1)
        trace.append(Event({DEFAULT_NAME_KEY: "CREDIT_SCORE_CHECK", DEFAULT_TIMESTAMP_KEY: a_start}))

        # AML check (async)
        aml_start = a_start + datetime.timedelta(seconds=2)
        trace.append(Event({DEFAULT_NAME_KEY: "AML_CHECK_ASYNC_START", DEFAULT_TIMESTAMP_KEY: aml_start}))
        aml_end = aml_start + datetime.timedelta(seconds=random.randint(2, 10))
        trace.append(Event({DEFAULT_NAME_KEY: "AML_CHECK_ASYNC_END", DEFAULT_TIMESTAMP_KEY: aml_end}))

        # Fraud check (async)
        fraud_start = aml_end + datetime.timedelta(seconds=2)
        trace.append(Event({DEFAULT_NAME_KEY: "FRAUD_CHECK_ASYNC_START", DEFAULT_TIMESTAMP_KEY: fraud_start}))
        fraud_end = fraud_start + datetime.timedelta(seconds=random.randint(2, 10))
        trace.append(Event({DEFAULT_NAME_KEY: "FRAUD_CHECK_ASYNC_END", DEFAULT_TIMESTAMP_KEY: fraud_end}))

        # CREDIT_CHECK_COMPLETE
        a_complete = fraud_end + datetime.timedelta(seconds=1)
        trace.append(Event({DEFAULT_NAME_KEY: "CREDIT_CHECK_COMPLETE", DEFAULT_TIMESTAMP_KEY: a_complete}))

        # Branch B: Cyclic Address Validation
        b_check = current_time + datetime.timedelta(seconds=1)
        trace.append(Event({DEFAULT_NAME_KEY: "ADDRESS_VALIDATION_CHECK", DEFAULT_TIMESTAMP_KEY: b_check}))
        loop_count = random.randint(0, 3)  # How many retries?
        for _lc in range(loop_count):
            b_task = b_check + datetime.timedelta(seconds=2)
            trace.append(Event({DEFAULT_NAME_KEY: "ADDRESS_VALIDATION_RETRY", DEFAULT_TIMESTAMP_KEY: b_task}))
            b_check = b_task + datetime.timedelta(seconds=1)
            trace.append(Event({DEFAULT_NAME_KEY: "ADDRESS_VALIDATION_CHECK", DEFAULT_TIMESTAMP_KEY: b_check}))

        b_complete = b_check + datetime.timedelta(seconds=1)
        trace.append(Event({DEFAULT_NAME_KEY: "ADDRESS_VALIDATION_COMPLETE", DEFAULT_TIMESTAMP_KEY: b_complete}))

        # JOIN (A and B branches)
        join_time = max(a_complete, b_complete) + datetime.timedelta(seconds=1)

        # FINAL_DECISION_CHECK
        final_check_time = join_time
        trace.append(Event({DEFAULT_NAME_KEY: "FINAL_DECISION_CHECK", DEFAULT_TIMESTAMP_KEY: final_check_time}))

        # Randomly decide if final decision triggers error (manual review) or leads to card issuance
        if random.random() < 0.2:
            # Error scenario
            error_time = final_check_time + datetime.timedelta(seconds=1)
            trace.append(Event({DEFAULT_NAME_KEY: "MANUAL_REVIEW", DEFAULT_TIMESTAMP_KEY: error_time}))
        else:
            # Success scenario
            complete_time = final_check_time + datetime.timedelta(seconds=1)
            trace.append(Event({DEFAULT_NAME_KEY: "CARD_ISSUED", DEFAULT_TIMESTAMP_KEY: complete_time}))

        log.append(trace)

    return log


if __name__ == "__main__":
    # 1. Generate synthetic event log
    event_log = generate_synthetic_log(num_traces=50)

    # Export to XES for inspection (optional)
    xes_exporter.export_log(event_log, "credit_card_process_log.xes")

    # 2. Convert event log to DataFrame
    from pm4py.objects.conversion.log import converter as log_converter

    event_df = log_converter.apply(event_log, variant=log_converter.Variants.TO_DATA_FRAME)

    # 3. Discover process model using Inductive Miner
    net, im, fm = inductive_miner.apply(event_log)

    # 4. Performance analysis: Compute average trace duration
    trace_durations = []
    for case_id, case_df in event_df.groupby("case:concept:name"):
        start_time = case_df["time:timestamp"].min()
        end_time = case_df["time:timestamp"].max()
        duration = (end_time - start_time).total_seconds()
        trace_durations.append(duration)
    avg_duration = sum(trace_durations) / len(trace_durations)
    print(f"Average trace duration: {avg_duration} seconds")

    # 5. Conformance Checking
    from pm4py.algo.filtering.log.variants import variants_filter

    # Get variants from the event_log
    variants_dict = variants_filter.get_variants(event_log)

    # Sort variants by frequency
    variant_freq = [(var, len(variants_dict[var])) for var in variants_dict]
    variant_freq.sort(key=lambda x: x[1], reverse=True)

    if len(variant_freq) >= 2:
        most_common_variant = variant_freq[0][0]
        least_common_variant = variant_freq[-1][0]
        print("Most common variant:", most_common_variant)
        print("Least common variant:", least_common_variant)


        def filter_variant(log, variant):
            from pm4py.objects.log.obj import EventLog
            filtered_log = EventLog()
            for t in log:
                act_seq = tuple([e['concept:name'] for e in t])
                if act_seq == variant:
                    filtered_log.append(t)
            return filtered_log


        most_common_sublog = filter_variant(event_log, most_common_variant)
        least_common_sublog = filter_variant(event_log, least_common_variant)

        alignment_most_common = alignments.apply(most_common_sublog, net, im, fm)
        alignment_least_common = alignments.apply(least_common_sublog, net, im, fm)

        fitness_most_common = alignment_eval.evaluate(alignment_most_common)
        fitness_least_common = alignment_eval.evaluate(alignment_least_common)

        print("Conformance fitness (most common variant):", fitness_most_common['average_trace_fitness'])
        print("Conformance fitness (least common variant):", fitness_least_common['average_trace_fitness'])
    else:
        print("Not enough distinct variants for a conformance check.")

    # Explanation (not printed, just as comments):
    # - Activities named to reflect credit card industry steps:
    #   * CUSTOMER_ID_VALIDATION before starting parallel tasks.
    #   * CREDIT_SCORE_CHECK, AML_CHECK (async), FRAUD_CHECK (async) represent parallel compliance and risk checks.
    #   * ADDRESS_VALIDATION_CHECK with ADDRESS_VALIDATION_RETRY loops mimic iterative data corrections.
    # - FINAL_DECISION_CHECK decides if we issue the card (CARD_ISSUED) or if it requires MANUAL_REVIEW.
    # - Inductive Miner is used to discover a process model that captures these complexities.
    # - Performance analysis calculates average durations of entire application processing.
    # - Conformance checking compares the most common variant to the least common variant to understand their fitness
    #   against the discovered model. This can help identify where unusual or rare process flows deviate from the norm.
