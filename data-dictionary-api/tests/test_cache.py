"""Unit tests for the dictionary cache."""
import pytest
import asyncio
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cache import DictionaryCache
from models import Dictionary, DictionaryEntry


@pytest.fixture
def sample_entry():
    """Create a sample dictionary entry."""
    return DictionaryEntry(
        preferred_name="account_number",
        description="Unique identifier for a financial account",
        examples=["ACC123456"],
        aliases=["accountNumber", "acc_num"]
    )


@pytest.fixture
def sample_dictionary(sample_entry):
    """Create a sample dictionary."""
    return Dictionary(
        entries={"account_number": sample_entry},
        version=1,
        last_updated=datetime.utcnow()
    )


@pytest.fixture
def cache_instance():
    """Create a fresh cache instance."""
    return DictionaryCache()


class TestDictionaryCache:
    """Tests for DictionaryCache class."""
    
    def test_initial_state(self, cache_instance):
        """Test cache starts empty."""
        assert cache_instance.dictionary is None
        assert cache_instance.get_all_entries() == {}
    
    def test_set_dictionary(self, cache_instance, sample_dictionary):
        """Test setting dictionary."""
        cache_instance.dictionary = sample_dictionary
        assert cache_instance.dictionary is not None
        assert len(cache_instance.get_all_entries()) == 1
    
    def test_get_entry(self, cache_instance, sample_dictionary):
        """Test getting a specific entry."""
        cache_instance.dictionary = sample_dictionary
        entry = cache_instance.get_entry("account_number")
        assert entry is not None
        assert entry.preferred_name == "account_number"
    
    def test_get_entry_not_found(self, cache_instance, sample_dictionary):
        """Test getting non-existent entry returns None."""
        cache_instance.dictionary = sample_dictionary
        entry = cache_instance.get_entry("nonexistent")
        assert entry is None
    
    def test_add_entry(self, cache_instance, sample_dictionary):
        """Test adding a new entry."""
        cache_instance.dictionary = sample_dictionary
        
        new_entry = DictionaryEntry(
            preferred_name="customer_name",
            description="Customer name",
            examples=[],
            aliases=["custName"]
        )
        
        cache_instance.add_entry(new_entry)
        
        assert cache_instance.get_entry("customer_name") is not None
        assert len(cache_instance.get_all_entries()) == 2
    
    def test_add_entry_updates_version(self, cache_instance, sample_dictionary):
        """Test that adding entry increments version."""
        cache_instance.dictionary = sample_dictionary
        old_version = cache_instance.dictionary.version
        
        new_entry = DictionaryEntry(
            preferred_name="test",
            description="Test",
            examples=[],
            aliases=[]
        )
        cache_instance.add_entry(new_entry)
        
        assert cache_instance.dictionary.version == old_version + 1
    
    def test_update_entry(self, cache_instance, sample_dictionary):
        """Test updating an existing entry."""
        cache_instance.dictionary = sample_dictionary
        
        updated = DictionaryEntry(
            preferred_name="account_number",
            description="Updated description",
            examples=["NEW123"],
            aliases=["newAlias"]
        )
        
        result = cache_instance.update_entry("account_number", updated)
        
        assert result is True
        entry = cache_instance.get_entry("account_number")
        assert entry.description == "Updated description"
    
    def test_update_entry_not_found(self, cache_instance, sample_dictionary):
        """Test updating non-existent entry returns False."""
        cache_instance.dictionary = sample_dictionary
        
        updated = DictionaryEntry(
            preferred_name="nonexistent",
            description="Test",
            examples=[],
            aliases=[]
        )
        
        result = cache_instance.update_entry("nonexistent", updated)
        assert result is False
    
    def test_delete_entry(self, cache_instance, sample_dictionary):
        """Test deleting an entry."""
        cache_instance.dictionary = sample_dictionary
        
        result = cache_instance.delete_entry("account_number")
        
        assert result is True
        assert cache_instance.get_entry("account_number") is None
        assert len(cache_instance.get_all_entries()) == 0
    
    def test_delete_entry_not_found(self, cache_instance, sample_dictionary):
        """Test deleting non-existent entry returns False."""
        cache_instance.dictionary = sample_dictionary
        result = cache_instance.delete_entry("nonexistent")
        assert result is False
    
    def test_add_alias(self, cache_instance, sample_dictionary):
        """Test adding an alias to an entry."""
        cache_instance.dictionary = sample_dictionary
        
        result = cache_instance.add_alias("account_number", "newAlias")
        
        assert result is True
        entry = cache_instance.get_entry("account_number")
        assert "newAlias" in entry.aliases
    
    def test_add_alias_duplicate(self, cache_instance, sample_dictionary):
        """Test adding duplicate alias doesn't duplicate."""
        cache_instance.dictionary = sample_dictionary
        
        cache_instance.add_alias("account_number", "acc_num")  # Already exists
        
        entry = cache_instance.get_entry("account_number")
        assert entry.aliases.count("acc_num") == 1
    
    def test_add_alias_entry_not_found(self, cache_instance, sample_dictionary):
        """Test adding alias to non-existent entry."""
        cache_instance.dictionary = sample_dictionary
        result = cache_instance.add_alias("nonexistent", "alias")
        assert result is False
    
    def test_remove_alias(self, cache_instance, sample_dictionary):
        """Test removing an alias."""
        cache_instance.dictionary = sample_dictionary
        
        result = cache_instance.remove_alias("account_number", "acc_num")
        
        assert result is True
        entry = cache_instance.get_entry("account_number")
        assert "acc_num" not in entry.aliases
    
    def test_remove_alias_not_found(self, cache_instance, sample_dictionary):
        """Test removing non-existent alias."""
        cache_instance.dictionary = sample_dictionary
        result = cache_instance.remove_alias("account_number", "nonexistent")
        assert result is False
    
    def test_thread_safety(self, cache_instance, sample_dictionary):
        """Test basic thread safety with lock."""
        cache_instance.dictionary = sample_dictionary
        
        # This just verifies the lock mechanism works
        with cache_instance._lock:
            entry = cache_instance._dictionary.entries.get("account_number")
            assert entry is not None


class TestCacheWithMatcher:
    """Tests for cache integration with matcher."""
    
    def test_matcher_updated_on_set(self, cache_instance, sample_dictionary):
        """Test that matcher is updated when dictionary is set."""
        cache_instance.dictionary = sample_dictionary
        
        # Import matcher to check it was updated
        from matcher import matcher
        results = matcher.find_matches("acc_num")
        assert len(results) >= 1
    
    def test_matcher_updated_on_add(self, cache_instance, sample_dictionary):
        """Test that matcher is updated when entry is added."""
        cache_instance.dictionary = sample_dictionary
        
        new_entry = DictionaryEntry(
            preferred_name="test_field",
            description="Test",
            examples=[],
            aliases=["testAlias"]
        )
        cache_instance.add_entry(new_entry)
        
        from matcher import matcher
        results = matcher.find_matches("testAlias")
        assert len(results) >= 1
        assert results[0].preferred_name == "test_field"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
