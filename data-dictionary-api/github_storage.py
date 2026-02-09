"""GitHub-based storage service for dictionary data."""
import base64
import json
from datetime import datetime
from typing import Optional, Tuple
import httpx

from config import settings
from models import Dictionary, DictionaryEntry


class GitHubStorage:
    """Service for storing and retrieving dictionary from GitHub."""
    
    BASE_URL = "https://api.github.com"
    RAW_URL = "https://raw.githubusercontent.com"
    
    def __init__(self):
        self._etag: Optional[str] = None
        self._last_sha: Optional[str] = None
    
    @property
    def _headers(self) -> dict:
        """Get headers for GitHub API requests."""
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DataDictionaryAPI/1.0"
        }
        if settings.github_token:
            headers["Authorization"] = f"Bearer {settings.github_token}"
        return headers
    
    @property
    def _raw_url(self) -> str:
        """Get the raw content URL for the dictionary file."""
        return f"{self.RAW_URL}/{settings.github_repo}/{settings.github_branch}/{settings.github_file_path}"
    
    @property
    def _api_url(self) -> str:
        """Get the API URL for the dictionary file."""
        return f"{self.BASE_URL}/repos/{settings.github_repo}/contents/{settings.github_file_path}"
    
    async def fetch_dictionary(self) -> Tuple[Optional[Dictionary], bool]:
        """Fetch dictionary from GitHub.
        
        Returns:
            Tuple of (Dictionary or None, has_changed bool)
            If has_changed is False and Dictionary is None, no update needed.
        """
        headers = self._headers.copy()
        if self._etag:
            headers["If-None-Match"] = self._etag
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(self._raw_url, headers=headers, timeout=10.0)
                
                if response.status_code == 304:
                    # Not modified
                    return None, False
                
                if response.status_code == 200:
                    self._etag = response.headers.get("ETag")
                    data = response.json()
                    return self._parse_dictionary(data), True
                
                # Handle errors
                print(f"GitHub fetch error: {response.status_code} - {response.text}")
                return None, False
                
            except Exception as e:
                print(f"GitHub fetch exception: {e}")
                return None, False
    
    async def get_file_info(self) -> Optional[dict]:
        """Get file metadata including SHA for updates."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    self._api_url,
                    headers=self._headers,
                    params={"ref": settings.github_branch},
                    timeout=10.0
                )
                if response.status_code == 200:
                    info = response.json()
                    self._last_sha = info.get("sha")
                    return info
                return None
            except Exception as e:
                print(f"GitHub get file info error: {e}")
                return None
    
    async def update_dictionary(self, dictionary: Dictionary) -> bool:
        """Update dictionary file on GitHub.
        
        Args:
            dictionary: The dictionary to save
            
        Returns:
            True if successful, False otherwise
        """
        # Get current SHA if we don't have it
        if not self._last_sha:
            await self.get_file_info()
        
        # Serialize dictionary
        content = self._serialize_dictionary(dictionary)
        content_b64 = base64.b64encode(content.encode()).decode()
        
        payload = {
            "message": f"Update dictionary - {datetime.utcnow().isoformat()}",
            "content": content_b64,
            "branch": settings.github_branch
        }
        
        if self._last_sha:
            payload["sha"] = self._last_sha
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    self._api_url,
                    headers=self._headers,
                    json=payload,
                    timeout=30.0
                )
                
                if response.status_code in (200, 201):
                    result = response.json()
                    self._last_sha = result.get("content", {}).get("sha")
                    self._etag = None  # Reset ETag to force refresh
                    return True
                
                print(f"GitHub update error: {response.status_code} - {response.text}")
                return False
                
            except Exception as e:
                print(f"GitHub update exception: {e}")
                return False
    
    def _parse_dictionary(self, data: dict) -> Dictionary:
        """Parse raw JSON data into Dictionary model."""
        entries = {}
        for name, entry_data in data.get("entries", {}).items():
            entries[name] = DictionaryEntry(
                preferred_name=name,
                description=entry_data.get("description", ""),
                examples=entry_data.get("examples", []),
                aliases=entry_data.get("aliases", [])
            )
        
        return Dictionary(
            entries=entries,
            version=data.get("version", 1),
            last_updated=data.get("last_updated", datetime.utcnow())
        )
    
    def _serialize_dictionary(self, dictionary: Dictionary) -> str:
        """Serialize Dictionary model to JSON string."""
        data = {
            "entries": {},
            "version": dictionary.version,
            "last_updated": dictionary.last_updated.isoformat()
        }
        
        for name, entry in dictionary.entries.items():
            data["entries"][name] = {
                "description": entry.description,
                "examples": entry.examples,
                "aliases": entry.aliases
            }
        
        return json.dumps(data, indent=2)


# Global storage instance
github_storage = GitHubStorage()
