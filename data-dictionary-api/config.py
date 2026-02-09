"""Configuration management for Data Dictionary API."""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # GitHub settings
    github_token: str = ""
    github_repo: str = ""  # e.g., "username/data-dictionary"
    github_file_path: str = "dictionary.json"
    github_branch: str = "main"
    
    # Cache settings
    cache_refresh_interval: int = 10  # seconds
    
    # Local file fallback
    local_file_path: str = "dictionary.json"
    
    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    @property
    def use_github(self) -> bool:
        """Check if GitHub storage is configured."""
        return bool(self.github_token and self.github_repo)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
