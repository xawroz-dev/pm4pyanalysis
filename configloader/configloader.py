import os
import time
import yaml
import pandas as pd
from datetime import datetime
from git import Repo  # pip install GitPython


##################################################################
# 1. GIT OPERATIONS
##################################################################

def clone_or_update_repo(repo_url: str, local_path: str):
    """
    Clone the Git repository if it doesn't exist locally;
    otherwise, pull the latest changes.
    """
    from git.exc import GitCommandError

    try:
        if not os.path.exists(local_path):
            print(f"[INFO] Cloning repository from '{repo_url}' to '{local_path}'...")
            Repo.clone_from(repo_url, local_path)
        else:
            print(f"[INFO] Pulling latest changes in '{local_path}'...")
            repo = Repo(local_path)
            origin = repo.remotes.origin
            origin.pull()
    except GitCommandError as e:
        print(f"[ERROR] Git command failed: {e}")


##################################################################
# 2. LOADING CONFIG & UTILITIES
##################################################################

def load_yaml_config(path: str) -> dict:
    """
    Load and parse a YAML config file, returning a dictionary.
    """
    if not os.path.exists(path):
        print(f"[ERROR] YAML config file not found: {path}")
        return {}
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        print(f"[ERROR] YAML parsing error in {path}: {exc}")
        return {}


def load_snapshot(snapshot_path: str) -> pd.DataFrame:
    """
    Load an existing snapshot, or return an empty DataFrame if not found.
    """
    if os.path.exists(snapshot_path):
        return pd.read_csv(snapshot_path)
    return pd.DataFrame()


def update_snapshot(snapshot_path: str, new_completed: pd.DataFrame, check_duplicates=True) -> pd.DataFrame:
    """
    Update the incremental snapshot with newly completed processes.
    Deduplicate if check_duplicates is True.
    """
    current_snapshot = load_snapshot(snapshot_path)

    if current_snapshot.empty:
        updated_snapshot = new_completed
    else:
        if check_duplicates:
            # Only append rows whose process_id is not already in the snapshot
            new_rows = new_completed[~new_completed["process_id"].isin(current_snapshot["process_id"])]
            updated_snapshot = pd.concat([current_snapshot, new_rows], ignore_index=True)
        else:
            updated_snapshot = pd.concat([current_snapshot, new_completed], ignore_index=True)

    # Save updated snapshot
    updated_snapshot.to_csv(snapshot_path, index=False)
    return updated_snapshot


##################################################################
# 3. PROCESS-MINING LOGIC
##################################################################

def get_completed_processes(event_logs: pd.DataFrame, start_activities, end_activities) -> pd.DataFrame:
    """
    Simple example: identify 'completed processes' by the presence of
    at least one start activity and at least one end activity for a
    given process_id. Adjust logic as needed.
    """
    started_ids = set(event_logs[event_logs["activity"].isin(start_activities)]["process_id"].unique())
    ended_ids = set(event_logs[event_logs["activity"].isin(end_activities)]["process_id"].unique())
    completed_ids = started_ids.intersection(ended_ids)

    # Return only logs belonging to completed process IDs
    completed_logs = event_logs[event_logs["process_id"].isin(completed_ids)]
    return completed_logs


def apply_filter_criteria(df: pd.DataFrame, criteria: dict) -> pd.DataFrame:
    """
    Given a dictionary of filter criteria (e.g., { department: "Sales", region: "APAC" }),
    filter the DataFrame to match those conditions.
    """
    filtered_df = df.copy()
    for key, value in criteria.items():
        # If needed, handle more complex logic (e.g., partial matches).
        filtered_df = filtered_df[filtered_df[key] == value]
    return filtered_df


def run_process_mining_for_usecase(usecase_config: dict, usecase_name: str):
    """
    Execute process-mining + snapshot creation for a single use case,
    if snapshot_creation is enabled.
    """

    # 1. Check global flag for snapshot creation
    snapshot_enabled = usecase_config.get("snapshot_creation", False)
    if not snapshot_enabled:
        print(f"[INFO] Use case '{usecase_name}': snapshot creation is disabled. Skipping.")
        return

    # 2. Extract config details
    label = usecase_config.get("label", usecase_name)  # If there's a special label, use it
    schedule = usecase_config.get("schedule", {})
    interval_hours = schedule.get("interval_hours", 3)  # you could store or use it differently

    process_definitions = usecase_config.get("process_definitions", {})
    start_activities = process_definitions.get("start_activities", [])
    end_activities = process_definitions.get("end_activities", [])

    source_data = usecase_config.get("source_data", {})
    event_log_path = source_data.get("path", "data/event_logs.csv")

    snapshot_conf = usecase_config.get("snapshot", {})
    snapshot_path = snapshot_conf.get("path", f"snapshots/{usecase_name}_snapshot.csv")

    additional_settings = usecase_config.get("additional_settings", {})
    check_dupes = additional_settings.get("check_duplicates", True)

    # 3. Load event logs
    if not os.path.exists(event_log_path):
        print(f"[WARNING] Event log file not found for use case '{usecase_name}': {event_log_path}")
        return
    event_logs = pd.read_csv(event_log_path)

    # 4. Identify completed processes (according to start/end activities)
    completed_df = get_completed_processes(event_logs, start_activities, end_activities)

    # 5. If you have multiple filter levels, apply them here
    filter_levels = usecase_config.get("filter_levels", [])
    if filter_levels:
        print(f"[INFO] Use case '{usecase_name}' has multiple filter levels defined.")
        for lvl in filter_levels:
            level_number = lvl.get("level")
            criteria = lvl.get("criteria", {})
            # Apply the filter to the completed_df or event_logs as needed
            # Below, we apply it to the completed_df just to demonstrate.
            filtered_data = apply_filter_criteria(completed_df, criteria)
            print(f"  > Filter level {level_number}, criteria: {criteria} -> {len(filtered_data)} records remain.")
            # You could optionally store each level of data in a separate snapshot or do further analysis.
            # This example just prints out the record count.

    # 6. Update snapshot with the union of all completed processes
    updated_snapshot = update_snapshot(snapshot_path, completed_df, check_duplicates=check_dupes)

    print(f"[INFO] [{label}] Use case '{usecase_name}' -> Snapshot updated. Total records: {len(updated_snapshot)}")


##################################################################
# 4. MAIN LOOP
##################################################################

def main():
    # ------------------------------------------------------ #
    # Adjust these to your environment:
    repo_url = "https://github.com/your-org/my-configs-repo.git"
    local_repo_path = "local-configs-repo"
    sleep_interval_global_hours = 6
    central_config_filename = "central_config.yaml"
    # ------------------------------------------------------ #

    while True:
        try:
            # 1. Get latest configs
            clone_or_update_repo(repo_url, local_repo_path)

            # 2. Load central config
            central_config_path = os.path.join(local_repo_path, central_config_filename)
            central_config = load_yaml_config(central_config_path)
            if not central_config:
                print("[ERROR] Central config is missing or empty.")
                print(f"[INFO] Sleeping {sleep_interval_global_hours} hours before retry...")
                time.sleep(sleep_interval_global_hours * 3600)
                continue

            # 3. Iterate over use cases
            use_cases = central_config.get("use_cases", [])
            for uc in use_cases:
                usecase_name = uc.get("name")
                active = uc.get("active", False)
                config_file = uc.get("config_file")

                # Skip if not active
                if not active:
                    print(f"[INFO] Use case '{usecase_name}' is marked inactive. Skipping.")
                    continue

                # Check config_file
                if not config_file:
                    print(f"[WARNING] No config_file specified for use case '{usecase_name}'. Skipping.")
                    continue

                # 4. Load use case’s individual YAML config
                usecase_config_path = os.path.join(local_repo_path, config_file)
                usecase_config = load_yaml_config(usecase_config_path)

                if not usecase_config:
                    print(f"[WARNING] Could not load config for use case '{usecase_name}'. Skipping.")
                    continue

                # 5. Run process mining for this use case
                run_process_mining_for_usecase(usecase_config, usecase_name)

        except Exception as e:
            print(f"[ERROR] Unhandled exception in main loop: {e}")

        # 6. Sleep until the next global cycle
        print(f"[INFO] Sleeping {sleep_interval_global_hours} hour(s) before the next cycle...\n")
        time.sleep(sleep_interval_global_hours * 3600)


if __name__ == "__main__":
    main()
