# job_scheduler.py

import schedule
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class JobScheduler:
    def __init__(self, process_mining_service, config):
        self.service = process_mining_service
        self.config = config

    def start(self):
        """
        Reads config for scheduling, sets up schedule tasks,
        then loops forever (like a daemon).
        """
        cron_expr = self.config["schedule"].get("cron")  # e.g. "0 2 * * *"
        # For simplicity, we might just do daily or every X minutes
        # We'll do every day at 2am logic with schedule library is a bit custom
        # We can do schedule.every().day.at("02:00").do(...) if that suits you

        if "cron" in self.config["schedule"]:
            # parse or we skip for brevity; let's do a daily approach:
            schedule.every().day.at("02:00").do(self.run_scheduled_job)
        else:
            # fallback: run every N hours
            schedule.every(6).hours.do(self.run_scheduled_job)

        logger.info("JobScheduler started. Running schedule loop.")
        while True:
            schedule.run_pending()
            time.sleep(60)  # check every minute

    def run_scheduled_job(self):
        """
        Called by schedule at the specified time. We'll define the time range:
        for example, last 'default_window_days' from config.
        """
        default_days = self.config["time_windows"].get("default_window_days", 7)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=default_days)

        logger.info(f"Running scheduled job for time range {start_time} to {end_time}")
        snapshot_data = self.service.run_analysis_for_time_range(start_time, end_time)
        logger.info(f"Scheduled job completed. Snapshot={snapshot_data['snapshot_id']}")
