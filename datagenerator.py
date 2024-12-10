import csv
import random
import datetime

NUM_CASES = 20
base_datetime = datetime.datetime(2024, 1, 1, 8, 0, 0)


def random_delay():
    return random.randint(1, 4)


def random_path():
    return random.random()


def generate_case_events(case_id):
    events = []
    current_time = base_datetime + datetime.timedelta(minutes=case_id * 10)

    def add_event(activity, lifecycle, resource="System", offset_sec=0, level_1=None, level_2=None, level_3=None):
        nonlocal current_time
        current_time += datetime.timedelta(seconds=offset_sec)
        events.append({
            "case_id": f"Case_{case_id}",
            "activity": activity,
            "timestamp": current_time.isoformat(),
            "resource": resource,
            "lifecycle": lifecycle,
            "level_1": level_1,
            "level_2": level_2,
            "level_3": level_3
        })

    # Consider a structure:
    # level_1 steps: A, B, C, D
    # Each can have multiple level_2 steps, and those can have multiple level_3 steps.
    # We'll just create a scenario similar to before:
    #
    # Level 1: A - Validation, B - Fraud Checks, C - Authorization, D - Settlement
    # Level 2 under A: A1 - Basic Checks, A2 - Advanced Checks
    # Level 3 under A1: A1a - Check Card Number, A1b - Check Expiry, etc.
    #
    # This allows filtering by:
    #   - Top-level: level_1 = "A"
    #   - Sub-level: level_1 = "A", level_2 = "A1"
    #   - Sub-sub-level: level_1 = "A", level_2 = "A1", level_3 = "A1a"
    #
    # Activities will be named simply but metadata fields store the hierarchy.

    # ---- LEVEL 1: A - Validation ----
    add_event("Start Validation", "start", level_1="A")
    add_event("Start Validation", "complete", level_1="A", offset_sec=random_delay())

    # LEVEL 2: A1 - Basic Checks
    add_event("Basic Checks", "start", level_1="A", level_2="A1", offset_sec=random_delay())
    # LEVEL 3 under A1:
    # A1a: Check Card Number
    add_event("Check Card Number", "start", level_1="A", level_2="A1", level_3="A1a", offset_sec=random_delay())
    add_event("Check Card Number", "complete", level_1="A", level_2="A1", level_3="A1a", offset_sec=random_delay())
    # Possibly repeat Card Number check multiple times:
    if random_path() < 0.3:
        times = random.randint(2, 4)
        for _ in range(times):
            add_event("Check Card Number", "start", level_1="A", level_2="A1", level_3="A1a", offset_sec=random_delay())
            add_event("Check Card Number", "complete", level_1="A", level_2="A1", level_3="A1a",
                      offset_sec=random_delay())

    # A1b: Check Expiry
    add_event("Check Expiry", "start", level_1="A", level_2="A1", level_3="A1b", offset_sec=random_delay())
    add_event("Check Expiry", "complete", level_1="A", level_2="A1", level_3="A1b", offset_sec=random_delay())
    # A1c: Check CVV
    add_event("Check CVV", "start", level_1="A", level_2="A1", level_3="A1c", offset_sec=random_delay())
    add_event("Check CVV", "complete", level_1="A", level_2="A1", level_3="A1c", offset_sec=random_delay())

    add_event("Basic Checks", "complete", level_1="A", level_2="A1", offset_sec=random_delay())

    # LEVEL 2: A2 - Advanced Checks (parallel)
    add_event("Advanced Checks", "start", level_1="A", level_2="A2", offset_sec=random_delay())
    # parallel: Address Check, Zip Check
    add_event("Validate Address", "start", level_1="A", level_2="A2", level_3="A2a", offset_sec=1)
    add_event("Validate Zip", "start", level_1="A", level_2="A2", level_3="A2b", offset_sec=1)
    add_event("Validate Address", "complete", level_1="A", level_2="A2", level_3="A2a", offset_sec=random_delay())
    add_event("Validate Zip", "complete", level_1="A", level_2="A2", level_3="A2b", offset_sec=random_delay())
    add_event("Advanced Checks", "complete", level_1="A", level_2="A2", offset_sec=random_delay())

    # Validation done

    # ---- LEVEL 1: B - Fraud Checks ----
    add_event("Fraud Checks", "start", level_1="B", offset_sec=random_delay())
    # Run Fraud Engine
    add_event("Run Fraud Engine", "start", level_1="B", level_2="B1", offset_sec=random_delay())
    add_event("Run Fraud Engine", "complete", level_1="B", level_2="B1", offset_sec=random_delay())

    # Async Fraud Notification
    fraud_notify_act = f"Fraud Notification Case {case_id}"
    add_event(fraud_notify_act, "start", level_1="B", level_2="B2", offset_sec=random_delay())
    add_event(fraud_notify_act, "complete", level_1="B", level_2="B2", offset_sec=random_delay())

    # Parallel checks: Geolocation, History
    add_event("Parallel Fraud Checks", "start", level_1="B", level_2="B3", offset_sec=random_delay())
    add_event("Check Geolocation", "start", level_1="B", level_2="B3", level_3="B3a", offset_sec=1)
    add_event("Check Transaction History", "start", level_1="B", level_2="B3", level_3="B3b", offset_sec=1)
    add_event("Check Geolocation", "complete", level_1="B", level_2="B3", level_3="B3a", offset_sec=random_delay())
    add_event("Check Transaction History", "complete", level_1="B", level_2="B3", level_3="B3b",
              offset_sec=random_delay())
    add_event("Parallel Fraud Checks", "complete", level_1="B", level_2="B3", offset_sec=random_delay())
    add_event("Fraud Checks", "complete", level_1="B", offset_sec=random_delay())

    # ---- LEVEL 1: C - Authorization ----
    add_event("Fund Authorization", "start", level_1="C", offset_sec=random_delay())
    bank_call_act = f"Bank API Call Case {case_id}"
    add_event(bank_call_act, "start", level_1="C", level_2="C1", offset_sec=random_delay())
    add_event(bank_call_act, "complete", level_1="C", level_2="C1", offset_sec=random_delay())

    add_event("Receive Bank Response", "start", level_1="C", level_2="C2", offset_sec=random_delay())
    # Gateway
    if random_path() < 0.4:
        # Negative response
        add_event("Receive Bank Response", "complete", level_1="C", level_2="C2", offset_sec=random_delay())
        add_event("Request Additional Info", "start", level_1="C", level_2="C3", offset_sec=random_delay())
        for i in range(random.randint(1, 2)):
            add_event("Request Additional Info", "complete", level_1="C", level_2="C3", offset_sec=random_delay())
            if i == 0 and random_path() < 0.5:
                # Repeat request
                add_event("Request Additional Info", "start", level_1="C", level_2="C3", offset_sec=random_delay())
    else:
        add_event("Receive Bank Response", "complete", level_1="C", level_2="C2", offset_sec=random_delay())

    add_event("Fund Authorization", "complete", level_1="C", offset_sec=random_delay())

    # ---- LEVEL 1: D - Settlement ----
    add_event("Settlement", "start", level_1="D", offset_sec=random_delay())
    # Charge Card steps (D1)
    charge_req_act = f"Charge Request Case {case_id}"
    add_event(charge_req_act, "start", level_1="D", level_2="D1", offset_sec=random_delay())
    add_event(charge_req_act, "complete", level_1="D", level_2="D1", offset_sec=random_delay())

    add_event("Receive Charge Response", "start", level_1="D", level_2="D2", offset_sec=random_delay())
    if random_path() < 0.3:
        # Error
        add_event("Receive Charge Response", "complete", level_1="D", level_2="D2", offset_sec=random_delay())
        add_event("Handle Charge Error", "start", level_1="D", level_2="D3", offset_sec=random_delay())
        add_event("Handle Charge Error", "complete", level_1="D", level_2="D3", offset_sec=random_delay())
        # Retry
        retry_act = f"Charge Request Retry Case {case_id}"
        add_event(retry_act, "start", level_1="D", level_2="D1", offset_sec=random_delay())
        add_event(retry_act, "complete", level_1="D", level_2="D1", offset_sec=random_delay())
        add_event("Receive Charge Response Retry", "start", level_1="D", level_2="D2", offset_sec=random_delay())
        add_event("Receive Charge Response Retry", "complete", level_1="D", level_2="D2", offset_sec=random_delay())
    else:
        add_event("Receive Charge Response", "complete", level_1="D", level_2="D2", offset_sec=random_delay())

    # Ledger Update and Notifications
    add_event("Ledger Update", "start", level_1="D", level_2="D4", offset_sec=random_delay())
    add_event("Insert Transaction Record", "start", level_1="D", level_2="D4", level_3="D4a", offset_sec=random_delay())
    add_event("Insert Transaction Record", "complete", level_1="D", level_2="D4", level_3="D4a",
              offset_sec=random_delay())
    add_event("Close Ledger Entry", "start", level_1="D", level_2="D4", level_3="D4b", offset_sec=random_delay())
    add_event("Close Ledger Entry", "complete", level_1="D", level_2="D4", level_3="D4b", offset_sec=random_delay())
    add_event("Ledger Update", "complete", level_1="D", level_2="D4", offset_sec=random_delay())

    # Async Notifications
    cust_notify = f"Customer Notification Case {case_id}"
    merch_notify = f"Merchant Notification Case {case_id}"
    add_event(cust_notify, "start", level_1="D", level_2="D5", offset_sec=random_delay())
    add_event(cust_notify, "complete", level_1="D", level_2="D5", offset_sec=random_delay())
    add_event(merch_notify, "start", level_1="D", level_2="D5", offset_sec=random_delay())
    add_event(merch_notify, "complete", level_1="D", level_2="D5", offset_sec=random_delay())

    # Batch Reconciliation (parallel)
    add_event("Batch Reconciliation", "start", level_1="D", level_2="D6", offset_sec=random_delay())
    add_event("Aggregate Transactions", "start", level_1="D", level_2="D6", level_3="D6a", offset_sec=1)
    add_event("Generate Batch Report", "start", level_1="D", level_2="D6", level_3="D6b", offset_sec=1)
    add_event("Aggregate Transactions", "complete", level_1="D", level_2="D6", level_3="D6a", offset_sec=random_delay())
    add_event("Generate Batch Report", "complete", level_1="D", level_2="D6", level_3="D6b", offset_sec=random_delay())
    add_event("Batch Reconciliation", "complete", level_1="D", level_2="D6", offset_sec=random_delay())

    add_event("Settlement", "complete", level_1="D", offset_sec=random_delay())

    return events


all_events = []
for c_id in range(1, NUM_CASES + 1):
    all_events.extend(generate_case_events(c_id))

# Sort events by timestamp
all_events.sort(key=lambda x: x["timestamp"])

with open("hierarchical_metadata_event_log.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["case_id", "activity", "timestamp", "resource", "lifecycle", "level_1",
                                           "level_2", "level_3"])
    writer.writeheader()
    for ev in all_events:
        writer.writerow(ev)

print("Event log with hierarchical metadata generated: hierarchical_metadata_event_log.csv")
