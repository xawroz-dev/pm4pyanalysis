"""FastAPI REST endpoints for Data Dictionary API."""
from fastapi import APIRouter, HTTPException, Query

from models import (
    DictionaryEntry, DictionaryEntryCreate, DictionaryEntryUpdate,
    AliasAdd, MatchResult, MatchResponse, MessageResponse
)
from cache import cache
from matcher import matcher

router = APIRouter()


# ============== Match Endpoints ==============

@router.get("/match/{query}", response_model=MatchResponse, tags=["Matching"])
async def match_term(
    query: str,
    top_k: int = Query(default=5, ge=1, le=20, description="Maximum number of results"),
    min_score: float = Query(default=30.0, ge=0, le=100, description="Minimum match score")
):
    """Find dictionary entries that fuzzy match the query term.
    
    Returns suggestions sorted by match score (highest first).
    Use this to find the preferred/canonical name for a variable.
    """
    matches = matcher.find_matches(query, top_k=top_k, min_score=min_score)
    return MatchResponse(query=query, matches=matches)


# ============== Entry CRUD Endpoints ==============

@router.get("/entries", response_model=dict[str, DictionaryEntry], tags=["Entries"])
async def list_entries():
    """List all dictionary entries."""
    return cache.get_all_entries()


@router.get("/entries/{name}", response_model=DictionaryEntry, tags=["Entries"])
async def get_entry(name: str):
    """Get a specific dictionary entry by preferred name."""
    entry = cache.get_entry(name)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Entry '{name}' not found")
    return entry


@router.post("/entries", response_model=DictionaryEntry, status_code=201, tags=["Entries"])
async def create_entry(entry: DictionaryEntryCreate):
    """Create a new dictionary entry.
    
    The preferred_name must be unique across all entries.
    """
    existing = cache.get_entry(entry.preferred_name)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Entry '{entry.preferred_name}' already exists"
        )
    
    new_entry = DictionaryEntry(
        preferred_name=entry.preferred_name,
        description=entry.description,
        examples=entry.examples,
        aliases=entry.aliases
    )
    cache.add_entry(new_entry)
    
    # Save to storage
    await cache.save()
    
    return new_entry


@router.put("/entries/{name}", response_model=DictionaryEntry, tags=["Entries"])
async def update_entry(name: str, update: DictionaryEntryUpdate):
    """Update an existing dictionary entry.
    
    Only provided fields will be updated. Set a field to null to keep existing value.
    """
    existing = cache.get_entry(name)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Entry '{name}' not found")
    
    # Apply updates
    updated_entry = DictionaryEntry(
        preferred_name=name,
        description=update.description if update.description is not None else existing.description,
        examples=update.examples if update.examples is not None else existing.examples,
        aliases=update.aliases if update.aliases is not None else existing.aliases
    )
    
    cache.update_entry(name, updated_entry)
    await cache.save()
    
    return updated_entry


@router.delete("/entries/{name}", response_model=MessageResponse, tags=["Entries"])
async def delete_entry(name: str):
    """Delete a dictionary entry."""
    if not cache.delete_entry(name):
        raise HTTPException(status_code=404, detail=f"Entry '{name}' not found")
    
    await cache.save()
    return MessageResponse(message=f"Entry '{name}' deleted successfully")


# ============== Alias Management Endpoints ==============

@router.post("/entries/{name}/aliases", response_model=DictionaryEntry, tags=["Aliases"])
async def add_alias(name: str, alias_data: AliasAdd):
    """Add an alias to an existing dictionary entry."""
    entry = cache.get_entry(name)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Entry '{name}' not found")
    
    if alias_data.alias in entry.aliases:
        raise HTTPException(
            status_code=409,
            detail=f"Alias '{alias_data.alias}' already exists for '{name}'"
        )
    
    cache.add_alias(name, alias_data.alias)
    await cache.save()
    
    return cache.get_entry(name)


@router.delete("/entries/{name}/aliases/{alias}", response_model=DictionaryEntry, tags=["Aliases"])
async def remove_alias(name: str, alias: str):
    """Remove an alias from a dictionary entry."""
    entry = cache.get_entry(name)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Entry '{name}' not found")
    
    if alias not in entry.aliases:
        raise HTTPException(
            status_code=404,
            detail=f"Alias '{alias}' not found for '{name}'"
        )
    
    cache.remove_alias(name, alias)
    await cache.save()
    
    return cache.get_entry(name)


# ============== Cache Management Endpoints ==============

@router.post("/refresh", response_model=MessageResponse, tags=["Cache"])
async def refresh_cache():
    """Force a refresh of the dictionary cache from storage."""
    refreshed = await cache.refresh()
    if refreshed:
        return MessageResponse(message="Cache refreshed successfully")
    return MessageResponse(message="No updates available", success=True)
