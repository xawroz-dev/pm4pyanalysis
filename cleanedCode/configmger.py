# config_manager.py

import os
import yaml
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, git_local_path="configs"):
        """
        :param git_local_path: Path to the local folder where the Git repo is cloned
        """
        self.git_local_path = git_local_path
        self._cached_config = None

    def load_config(self):
        """
        Reads a YAML config file from the local Git repo path.
        Example config might define:
          - schedule intervals
          - index names
          - attribute definitions (first-level, second-level)
          - job parameters
        """
        config_file = os.path.join(self.git_local_path, "my_config.yml")
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file not found: {config_file}")

        with open(config_file, "r") as f:
            self._cached_config = yaml.safe_load(f)

        logger.info(f"Loaded config from {config_file}")
        return self._cached_config

    def get_config(self):
        if not self._cached_config:
            return self.load_config()
        return self._cached_config

    def refresh_from_git(self):
        """
        (Optional) logic to pull latest changes from Git remote,
        then reload config. We'll skip actual Git commands for brevity.
        """
        # e.g. 'git pull' in self.git_local_path
        logger.info("Pulling updates from Git (placeholder).")
        # after pulling:
        self.load_config()
