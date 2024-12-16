import pandas as pd
import random
from datetime import datetime, timedelta


# Helper function to generate random timestamps
def random_timestamp(base_time, max_duration=10):
    return base_time + timedelta(seconds=random.randint(0, 3))


# Define process instances with parallel, cyclic, and asynchronous activities
def generate_event_log():
    data = []
    parallel_activities = [f"Parallel_{i}" for i in range(1, 10)]  # 8-9 parallel activities

    for i in range(1, 11):  # 10 process instances
        process_id = f"case_{i}"
        start_time = datetime(2024, 12, 1, 8, 0) + timedelta(hours=i)
        current_time = start_time

        # Sequential activity
        data.append([process_id, "Start", current_time, "start"])
        current_time = random_timestamp(current_time)

        # Randomize the order of parallel activities
        randomized_parallel_activities = random.sample(parallel_activities, len(parallel_activities))
        for activity in randomized_parallel_activities:
            data.append([process_id, activity, random_timestamp(current_time), "parallel"])
        current_time = random_timestamp(current_time)
        current_time = random_timestamp(current_time)
        current_time = random_timestamp(current_time)
        current_time = random_timestamp(current_time)

        # Cyclic activity
        for j in range(random.randint(1, 3)):  # Add a cycle
            data.append([process_id, f"Cyclic_{j + 1}", random_timestamp(current_time), "cyclic"])
            current_time = random_timestamp(current_time)

        # Asynchronous activity
        async_call_time = random_timestamp(current_time)
        data.append([process_id, "Async_Call_Start", async_call_time, "async"])
        data.append([process_id, "Async_Call_End", random_timestamp(async_call_time, 15), "async"])

        # Error boundary (optional activity)
        if random.random() > 0.7:  # 30% chance of error
            data.append([process_id, "Error_Handler", random_timestamp(current_time), "error"])

        # End activity
        data.append([process_id, "End", random_timestamp(current_time), "end"])

    return pd.DataFrame(data, columns=["case_id", "activity", "timestamp", "type"])


# Generate the synthetic event log
event_log = generate_event_log()

# Save the log as a CSV for PM4py usage
event_log.to_csv("synthetic_event_log.csv", index=False)

# Preview the generated event log
print(event_log.head(20))
