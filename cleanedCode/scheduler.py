# app.py

import logging
import sys

from config_manager import ConfigManager
from storage import GlobalStorage
from process_mining_service import ProcessMiningService
from job_scheduler import JobScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # 1) Load config from Git or local
    config_mgr = ConfigManager(git_local_path="configs")
    config = config_mgr.get_config()  # e.g. from my_config.yml

    # 2) Initialize storage
    storage = GlobalStorage()

    # 3) Create the process mining service
    pm_service = ProcessMiningService(storage, config)

    # 4) Setup job scheduler, or run once
    scheduler = JobScheduler(pm_service, config)

    # For demonstration, let's either run once or start the schedule
    if "--run-once" in sys.argv:
        # run immediate job
        from datetime import datetime, timedelta
        end_time = datetime.now()
        start_time = end_time - timedelta(days=config["time_windows"]["default_window_days"])
        snapshot = pm_service.run_analysis_for_time_range(start_time, end_time)
        logger.info(f"One-time analysis snapshot: {snapshot['snapshot_id']}")
    else:
        # Start schedule loop
        scheduler.start()

if __name__ == "__main__":
    main()
