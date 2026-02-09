"""Unit tests for the fuzzy matcher."""
import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matcher import FuzzyMatcher
from models import DictionaryEntry


@pytest.fixture
def sample_entries():
    """Create sample dictionary entries for testing."""
    return {
        "account_number": DictionaryEntry(
            preferred_name="account_number",
            description="Unique identifier for a financial account",
            examples=["ACC123456"],
            aliases=["accountNumber", "acc_num", "acnt_num", "accNumber", "ACCT_NO"]
        ),
        "customer_name": DictionaryEntry(
            preferred_name="customer_name",
            description="Full name of the customer",
            examples=["John Doe"],
            aliases=["customerName", "cust_name", "CUST_NAME", "clientName"]
        ),
        "transaction_amount": DictionaryEntry(
            preferred_name="transaction_amount",
            description="Dollar amount of a transaction",
            examples=["100.00"],
            aliases=["transactionAmount", "txn_amt", "TXN_AMT", "txnAmount"]
        ),
        "annual_percentage_rate": DictionaryEntry(
            preferred_name="annual_percentage_rate",
            description="Yearly interest rate",
            examples=["18.99%"],
            aliases=["APR", "interest_rate", "INT_RATE"]
        )
    }


@pytest.fixture
def matcher(sample_entries):
    """Create a matcher with sample entries."""
    m = FuzzyMatcher()
    m.update_entries(sample_entries)
    return m


class TestFuzzyMatcher:
    """Tests for FuzzyMatcher class."""
    
    def test_exact_match_preferred_name(self, matcher):
        """Test exact match on preferred name."""
        results = matcher.find_matches("account_number")
        assert len(results) == 1
        assert results[0].preferred_name == "account_number"
        assert results[0].score == 100.0
    
    def test_exact_match_alias(self, matcher):
        """Test exact match on an alias."""
        results = matcher.find_matches("acc_num")
        assert len(results) == 1
        assert results[0].preferred_name == "account_number"
        assert results[0].score == 100.0
    
    def test_exact_match_case_insensitive(self, matcher):
        """Test exact match is case insensitive."""
        results = matcher.find_matches("ACCOUNT_NUMBER")
        assert len(results) == 1
        assert results[0].preferred_name == "account_number"
        assert results[0].score == 100.0
    
    def test_camel_case_match(self, matcher):
        """Test matching camelCase variations."""
        results = matcher.find_matches("accountNumber")
        assert len(results) >= 1
        assert results[0].preferred_name == "account_number"
    
    def test_partial_match(self, matcher):
        """Test partial/fuzzy matching."""
        results = matcher.find_matches("acct_num")
        assert len(results) >= 1
        # Should find account_number as a close match
        found = any(r.preferred_name == "account_number" for r in results)
        assert found
    
    def test_typo_match(self, matcher):
        """Test matching with typos."""
        results = matcher.find_matches("accoun_number")
        assert len(results) >= 1
        assert results[0].preferred_name == "account_number"
    
    def test_upper_case_alias(self, matcher):
        """Test matching UPPER_CASE aliases."""
        results = matcher.find_matches("TXN_AMT")
        assert len(results) == 1
        assert results[0].preferred_name == "transaction_amount"
        assert results[0].score == 100.0
    
    def test_multiple_results(self, matcher):
        """Test that multiple results can be returned."""
        results = matcher.find_matches("name", top_k=5)
        assert len(results) >= 1
        # Should find customer_name
        found = any(r.preferred_name == "customer_name" for r in results)
        assert found
    
    def test_min_score_filter(self, matcher):
        """Test minimum score filtering."""
        results = matcher.find_matches("xyz123", min_score=80.0)
        # Random string should not match anything above 80%
        assert len(results) == 0
    
    def test_top_k_limit(self, matcher):
        """Test top_k limits results."""
        results = matcher.find_matches("a", top_k=2)
        assert len(results) <= 2
    
    def test_empty_entries(self):
        """Test matching with no entries."""
        m = FuzzyMatcher()
        results = m.find_matches("anything")
        assert len(results) == 0
    
    def test_matched_on_field(self, matcher):
        """Test that matched_on shows which alias matched."""
        results = matcher.find_matches("acc_num")
        assert len(results) >= 1
        assert results[0].matched_on == "acc_num"
    
    def test_normalize_camel_case(self, matcher):
        """Test camelCase normalization."""
        normalized = matcher._normalize("accountNumber")
        assert normalized == "account number"
    
    def test_normalize_snake_case(self, matcher):
        """Test snake_case normalization."""
        normalized = matcher._normalize("account_number")
        assert normalized == "account number"
    
    def test_normalize_mixed(self, matcher):
        """Test mixed case normalization."""
        normalized = matcher._normalize("AcctNUM_value")
        assert "acct" in normalized.lower()
        assert "num" in normalized.lower()


class TestMatcherUpdate:
    """Tests for updating matcher entries."""
    
    def test_update_entries(self, sample_entries):
        """Test updating entries."""
        m = FuzzyMatcher()
        m.update_entries(sample_entries)
        
        results = m.find_matches("account_number")
        assert len(results) == 1
    
    def test_update_clears_old_entries(self, sample_entries):
        """Test that update clears old entries."""
        m = FuzzyMatcher()
        m.update_entries(sample_entries)
        
        # Update with empty
        m.update_entries({})
        results = m.find_matches("account_number")
        assert len(results) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
