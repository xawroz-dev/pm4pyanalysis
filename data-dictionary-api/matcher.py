"""Fuzzy matching service using RapidFuzz."""
from rapidfuzz import fuzz, process
from models import DictionaryEntry, MatchResult


class FuzzyMatcher:
    """Fuzzy matching engine for finding similar dictionary entries."""
    
    def __init__(self):
        self._entries: dict[str, DictionaryEntry] = {}
        self._alias_to_entry: dict[str, str] = {}  # Maps alias -> preferred_name
    
    def update_entries(self, entries: dict[str, DictionaryEntry]) -> None:
        """Update the matcher with new dictionary entries."""
        self._entries = entries
        self._alias_to_entry = {}
        
        for preferred_name, entry in entries.items():
            # Map the preferred name itself
            self._alias_to_entry[preferred_name.lower()] = preferred_name
            # Map all aliases
            for alias in entry.aliases:
                self._alias_to_entry[alias.lower()] = preferred_name
    
    def _normalize(self, text: str) -> str:
        """Normalize text for matching (handle camelCase, snake_case, etc.)."""
        # Convert camelCase to spaces
        result = []
        for i, char in enumerate(text):
            if char.isupper() and i > 0 and text[i-1].islower():
                result.append(' ')
            result.append(char.lower())
        
        # Replace underscores and hyphens with spaces
        normalized = ''.join(result).replace('_', ' ').replace('-', ' ')
        return ' '.join(normalized.split())  # Collapse multiple spaces
    
    def _calculate_score(self, query: str, candidate: str) -> float:
        """Calculate combined similarity score using multiple algorithms."""
        # Normalize both strings
        norm_query = self._normalize(query)
        norm_candidate = self._normalize(candidate)
        
        # Calculate various scores
        ratio = fuzz.ratio(norm_query, norm_candidate)
        partial = fuzz.partial_ratio(norm_query, norm_candidate)
        token_sort = fuzz.token_sort_ratio(norm_query, norm_candidate)
        token_set = fuzz.token_set_ratio(norm_query, norm_candidate)
        
        # Weighted combination (prioritize exact and partial matches)
        score = (ratio * 0.3 + partial * 0.3 + token_sort * 0.2 + token_set * 0.2)
        
        # Bonus for exact case-insensitive match
        if query.lower() == candidate.lower():
            score = 100.0
        
        return min(score, 100.0)
    
    def find_matches(self, query: str, top_k: int = 5, min_score: float = 30.0) -> list[MatchResult]:
        """Find the closest matching dictionary entries for a query.
        
        Args:
            query: The input term to match
            top_k: Maximum number of results to return
            min_score: Minimum score threshold (0-100)
        
        Returns:
            List of MatchResult objects sorted by score (descending)
        """
        if not self._entries:
            return []
        
        # Check for exact match first
        query_lower = query.lower()
        if query_lower in self._alias_to_entry:
            preferred = self._alias_to_entry[query_lower]
            entry = self._entries[preferred]
            return [MatchResult(
                preferred_name=preferred,
                score=100.0,
                description=entry.description,
                aliases=entry.aliases,
                matched_on=query
            )]
        
        # Calculate scores for all aliases
        candidates: list[tuple[str, str, float]] = []  # (alias, preferred_name, score)
        
        for alias, preferred_name in self._alias_to_entry.items():
            score = self._calculate_score(query, alias)
            if score >= min_score:
                candidates.append((alias, preferred_name, score))
        
        # Sort by score descending
        candidates.sort(key=lambda x: x[2], reverse=True)
        
        # Deduplicate by preferred_name (keep highest scoring alias)
        seen_preferred: set[str] = set()
        results: list[MatchResult] = []
        
        for alias, preferred_name, score in candidates:
            if preferred_name in seen_preferred:
                continue
            seen_preferred.add(preferred_name)
            
            entry = self._entries[preferred_name]
            results.append(MatchResult(
                preferred_name=preferred_name,
                score=round(score, 2),
                description=entry.description,
                aliases=entry.aliases,
                matched_on=alias
            ))
            
            if len(results) >= top_k:
                break
        
        return results


# Global matcher instance
matcher = FuzzyMatcher()
