"""Integration tests for the API endpoints."""
import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from main import app
from cache import cache
from models import Dictionary, DictionaryEntry
from datetime import datetime


@pytest.fixture(autouse=True)
def reset_cache():
    """Reset cache before each test."""
    # Set up a test dictionary
    test_entries = {
        "account_number": DictionaryEntry(
            preferred_name="account_number",
            description="Unique identifier for a financial account",
            examples=["ACC123456"],
            aliases=["accountNumber", "acc_num", "ACCT_NO"]
        ),
        "customer_name": DictionaryEntry(
            preferred_name="customer_name",
            description="Full name of the customer",
            examples=["John Doe"],
            aliases=["customerName", "cust_name"]
        )
    }
    cache.dictionary = Dictionary(
        entries=test_entries,
        version=1,
        last_updated=datetime.utcnow()
    )
    yield
    # Cleanup
    cache._dictionary = None


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


class TestHealthEndpoints:
    """Tests for health check endpoints."""
    
    def test_root(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "entries_count" in data
    
    def test_health(self, client):
        """Test health endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"


class TestMatchEndpoint:
    """Tests for fuzzy matching endpoint."""
    
    def test_exact_match(self, client):
        """Test exact match."""
        response = client.get("/match/account_number")
        assert response.status_code == 200
        data = response.json()
        assert data["query"] == "account_number"
        assert len(data["matches"]) >= 1
        assert data["matches"][0]["preferred_name"] == "account_number"
        assert data["matches"][0]["score"] == 100.0
    
    def test_alias_match(self, client):
        """Test match on alias."""
        response = client.get("/match/acc_num")
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) >= 1
        assert data["matches"][0]["preferred_name"] == "account_number"
    
    def test_fuzzy_match(self, client):
        """Test fuzzy matching."""
        response = client.get("/match/acct_number")
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) >= 1
    
    def test_match_with_top_k(self, client):
        """Test top_k parameter."""
        response = client.get("/match/name?top_k=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) <= 1
    
    def test_match_with_min_score(self, client):
        """Test min_score parameter."""
        response = client.get("/match/xyz123?min_score=90")
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) == 0
    
    def test_match_response_structure(self, client):
        """Test response structure includes all fields."""
        response = client.get("/match/acc_num")
        assert response.status_code == 200
        data = response.json()
        
        match = data["matches"][0]
        assert "preferred_name" in match
        assert "score" in match
        assert "description" in match
        assert "aliases" in match
        assert "matched_on" in match


class TestListEntriesEndpoint:
    """Tests for listing entries."""
    
    def test_list_entries(self, client):
        """Test listing all entries."""
        response = client.get("/entries")
        assert response.status_code == 200
        data = response.json()
        assert "account_number" in data
        assert "customer_name" in data


class TestGetEntryEndpoint:
    """Tests for getting a single entry."""
    
    def test_get_entry(self, client):
        """Test getting an entry."""
        response = client.get("/entries/account_number")
        assert response.status_code == 200
        data = response.json()
        assert data["preferred_name"] == "account_number"
        assert "description" in data
        assert "aliases" in data
    
    def test_get_entry_not_found(self, client):
        """Test getting non-existent entry."""
        response = client.get("/entries/nonexistent")
        assert response.status_code == 404


class TestCreateEntryEndpoint:
    """Tests for creating entries."""
    
    def test_create_entry(self, client):
        """Test creating a new entry."""
        payload = {
            "preferred_name": "new_field",
            "description": "A new test field",
            "examples": ["example1"],
            "aliases": ["newField", "NEW_FIELD"]
        }
        
        response = client.post("/entries", json=payload)
        assert response.status_code == 201
        data = response.json()
        assert data["preferred_name"] == "new_field"
        assert data["description"] == "A new test field"
    
    def test_create_entry_conflict(self, client):
        """Test creating duplicate entry returns 409."""
        payload = {
            "preferred_name": "account_number",  # Already exists
            "description": "Duplicate",
            "examples": [],
            "aliases": []
        }
        
        response = client.post("/entries", json=payload)
        assert response.status_code == 409
    
    def test_create_entry_validation(self, client):
        """Test validation on create."""
        payload = {
            "preferred_name": "",  # Invalid - empty
            "description": "Test",
            "examples": [],
            "aliases": []
        }
        
        response = client.post("/entries", json=payload)
        assert response.status_code == 422  # Validation error


class TestUpdateEntryEndpoint:
    """Tests for updating entries."""
    
    def test_update_entry(self, client):
        """Test updating an entry."""
        payload = {
            "description": "Updated description"
        }
        
        response = client.put("/entries/account_number", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "Updated description"
    
    def test_update_entry_not_found(self, client):
        """Test updating non-existent entry."""
        payload = {"description": "Test"}
        response = client.put("/entries/nonexistent", json=payload)
        assert response.status_code == 404
    
    def test_update_preserves_unset_fields(self, client):
        """Test that unset fields are preserved."""
        # Only update description, aliases should remain
        payload = {"description": "New description"}
        
        response = client.put("/entries/account_number", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert len(data["aliases"]) > 0  # Aliases preserved


class TestDeleteEntryEndpoint:
    """Tests for deleting entries."""
    
    def test_delete_entry(self, client):
        """Test deleting an entry."""
        response = client.delete("/entries/account_number")
        assert response.status_code == 200
        
        # Verify it's gone
        response = client.get("/entries/account_number")
        assert response.status_code == 404
    
    def test_delete_entry_not_found(self, client):
        """Test deleting non-existent entry."""
        response = client.delete("/entries/nonexistent")
        assert response.status_code == 404


class TestAliasEndpoints:
    """Tests for alias management endpoints."""
    
    def test_add_alias(self, client):
        """Test adding an alias."""
        payload = {"alias": "newAlias"}
        
        response = client.post("/entries/account_number/aliases", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "newAlias" in data["aliases"]
    
    def test_add_alias_duplicate(self, client):
        """Test adding duplicate alias returns 409."""
        payload = {"alias": "acc_num"}  # Already exists
        
        response = client.post("/entries/account_number/aliases", json=payload)
        assert response.status_code == 409
    
    def test_add_alias_entry_not_found(self, client):
        """Test adding alias to non-existent entry."""
        payload = {"alias": "test"}
        response = client.post("/entries/nonexistent/aliases", json=payload)
        assert response.status_code == 404
    
    def test_remove_alias(self, client):
        """Test removing an alias."""
        response = client.delete("/entries/account_number/aliases/acc_num")
        assert response.status_code == 200
        data = response.json()
        assert "acc_num" not in data["aliases"]
    
    def test_remove_alias_not_found(self, client):
        """Test removing non-existent alias."""
        response = client.delete("/entries/account_number/aliases/nonexistent")
        assert response.status_code == 404


class TestRefreshEndpoint:
    """Tests for cache refresh endpoint."""
    
    def test_refresh(self, client):
        """Test refresh endpoint."""
        response = client.post("/refresh")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
