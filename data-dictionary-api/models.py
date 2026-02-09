"""Pydantic models for Data Dictionary API."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class DictionaryEntry(BaseModel):
    """A single dictionary entry with preferred name and aliases."""
    preferred_name: str = Field(..., description="The canonical/preferred name")
    description: str = Field(..., description="Description of what this term represents")
    examples: list[str] = Field(default_factory=list, description="Example values")
    aliases: list[str] = Field(default_factory=list, description="Alternative names/aliases")


class DictionaryEntryCreate(BaseModel):
    """Request model for creating a new dictionary entry."""
    preferred_name: str = Field(..., min_length=1, description="The canonical/preferred name")
    description: str = Field(..., min_length=1, description="Description of what this term represents")
    examples: list[str] = Field(default_factory=list, description="Example values")
    aliases: list[str] = Field(default_factory=list, description="Alternative names/aliases")


class DictionaryEntryUpdate(BaseModel):
    """Request model for updating a dictionary entry."""
    description: Optional[str] = Field(None, description="Updated description")
    examples: Optional[list[str]] = Field(None, description="Updated examples")
    aliases: Optional[list[str]] = Field(None, description="Updated aliases (replaces existing)")


class AliasAdd(BaseModel):
    """Request model for adding an alias."""
    alias: str = Field(..., min_length=1, description="The alias to add")


class MatchResult(BaseModel):
    """Result of a fuzzy match operation."""
    preferred_name: str
    score: float = Field(..., ge=0, le=100, description="Match score (0-100)")
    description: str
    aliases: list[str]
    matched_on: str = Field(..., description="The alias or name that matched")


class MatchResponse(BaseModel):
    """Response for match endpoint."""
    query: str
    matches: list[MatchResult]


class Dictionary(BaseModel):
    """The complete dictionary structure."""
    entries: dict[str, DictionaryEntry] = Field(default_factory=dict)
    version: int = Field(default=1)
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class MessageResponse(BaseModel):
    """Generic message response."""
    message: str
    success: bool = True
