"""In-memory cache with background refresh for dictionary data."""
import asyncio
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import settings
from models import Dictionary, DictionaryEntry
from github_storage import github_storage
from matcher import matcher


class DictionaryCache:
    """Thread-safe in-memory cache for dictionary with auto-refresh."""
    
    def __init__(self):
        self._dictionary: Optional[Dictionary] = None
        self._lock = threading.RLock()
        self._refresh_task: Optional[asyncio.Task] = None
        self._running = False
    
    @property
    def dictionary(self) -> Optional[Dictionary]:
        """Get the current dictionary (thread-safe)."""
        with self._lock:
            return self._dictionary
    
    @dictionary.setter
    def dictionary(self, value: Dictionary) -> None:
        """Set the dictionary and update matcher (thread-safe)."""
        with self._lock:
            self._dictionary = value
            # Update the fuzzy matcher with new entries
            matcher.update_entries(value.entries)
    
    def get_entry(self, name: str) -> Optional[DictionaryEntry]:
        """Get a specific entry by preferred name."""
        with self._lock:
            if self._dictionary:
                return self._dictionary.entries.get(name)
            return None
    
    def get_all_entries(self) -> dict[str, DictionaryEntry]:
        """Get all entries."""
        with self._lock:
            if self._dictionary:
                return self._dictionary.entries.copy()
            return {}
    
    def add_entry(self, entry: DictionaryEntry) -> None:
        """Add a new entry to the dictionary."""
        with self._lock:
            if self._dictionary is None:
                self._dictionary = Dictionary()
            
            self._dictionary.entries[entry.preferred_name] = entry
            self._dictionary.version += 1
            self._dictionary.last_updated = datetime.utcnow()
            matcher.update_entries(self._dictionary.entries)
    
    def update_entry(self, name: str, entry: DictionaryEntry) -> bool:
        """Update an existing entry."""
        with self._lock:
            if self._dictionary is None or name not in self._dictionary.entries:
                return False
            
            self._dictionary.entries[name] = entry
            self._dictionary.version += 1
            self._dictionary.last_updated = datetime.utcnow()
            matcher.update_entries(self._dictionary.entries)
            return True
    
    def delete_entry(self, name: str) -> bool:
        """Delete an entry by name."""
        with self._lock:
            if self._dictionary is None or name not in self._dictionary.entries:
                return False
            
            del self._dictionary.entries[name]
            self._dictionary.version += 1
            self._dictionary.last_updated = datetime.utcnow()
            matcher.update_entries(self._dictionary.entries)
            return True
    
    def add_alias(self, name: str, alias: str) -> bool:
        """Add an alias to an entry."""
        with self._lock:
            if self._dictionary is None or name not in self._dictionary.entries:
                return False
            
            entry = self._dictionary.entries[name]
            if alias not in entry.aliases:
                entry.aliases.append(alias)
                self._dictionary.version += 1
                self._dictionary.last_updated = datetime.utcnow()
                matcher.update_entries(self._dictionary.entries)
            return True
    
    def remove_alias(self, name: str, alias: str) -> bool:
        """Remove an alias from an entry."""
        with self._lock:
            if self._dictionary is None or name not in self._dictionary.entries:
                return False
            
            entry = self._dictionary.entries[name]
            if alias in entry.aliases:
                entry.aliases.remove(alias)
                self._dictionary.version += 1
                self._dictionary.last_updated = datetime.utcnow()
                matcher.update_entries(self._dictionary.entries)
                return True
            return False
    
    async def load_initial(self) -> None:
        """Load dictionary on startup."""
        if settings.use_github:
            dictionary, _ = await github_storage.fetch_dictionary()
            if dictionary:
                self.dictionary = dictionary
                print(f"Loaded {len(dictionary.entries)} entries from GitHub")
                return
        
        # Fallback to local file
        await self._load_from_local()
    
    async def _load_from_local(self) -> None:
        """Load dictionary from local file."""
        local_path = Path(settings.local_file_path)
        if local_path.exists():
            try:
                with open(local_path, 'r') as f:
                    data = json.load(f)
                
                entries = {}
                for name, entry_data in data.get("entries", {}).items():
                    entries[name] = DictionaryEntry(
                        preferred_name=name,
                        description=entry_data.get("description", ""),
                        examples=entry_data.get("examples", []),
                        aliases=entry_data.get("aliases", [])
                    )
                
                self.dictionary = Dictionary(
                    entries=entries,
                    version=data.get("version", 1),
                    last_updated=data.get("last_updated", datetime.utcnow())
                )
                print(f"Loaded {len(entries)} entries from local file")
            except Exception as e:
                print(f"Error loading local file: {e}")
                self.dictionary = Dictionary()
        else:
            print("No existing dictionary found, starting fresh")
            self.dictionary = Dictionary()
    
    async def save_to_local(self) -> None:
        """Save dictionary to local file."""
        if self._dictionary is None:
            return
        
        with self._lock:
            data = {
                "entries": {},
                "version": self._dictionary.version,
                "last_updated": self._dictionary.last_updated.isoformat()
            }
            
            for name, entry in self._dictionary.entries.items():
                data["entries"][name] = {
                    "description": entry.description,
                    "examples": entry.examples,
                    "aliases": entry.aliases
                }
        
        local_path = Path(settings.local_file_path)
        with open(local_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    async def save(self) -> bool:
        """Save dictionary to storage (GitHub or local)."""
        if self._dictionary is None:
            return False
        
        if settings.use_github:
            success = await github_storage.update_dictionary(self._dictionary)
            if success:
                return True
            # Fallback to local on failure
            print("GitHub save failed, saving locally")
        
        await self.save_to_local()
        return True
    
    async def refresh(self) -> bool:
        """Refresh dictionary from GitHub if updated."""
        if not settings.use_github:
            return False
        
        dictionary, changed = await github_storage.fetch_dictionary()
        if changed and dictionary:
            self.dictionary = dictionary
            print(f"Refreshed dictionary: {len(dictionary.entries)} entries")
            return True
        return False
    
    async def start_background_refresh(self) -> None:
        """Start the background refresh loop."""
        self._running = True
        self._refresh_task = asyncio.create_task(self._refresh_loop())
    
    async def stop_background_refresh(self) -> None:
        """Stop the background refresh loop."""
        self._running = False
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
    
    async def _refresh_loop(self) -> None:
        """Background loop that refreshes dictionary periodically."""
        while self._running:
            await asyncio.sleep(settings.cache_refresh_interval)
            try:
                await self.refresh()
            except Exception as e:
                print(f"Background refresh error: {e}")


# Global cache instance
cache = DictionaryCache()
