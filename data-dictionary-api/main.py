"""Main application entry point for Data Dictionary API."""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from cache import cache
from api import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup and shutdown."""
    # Startup
    print("Starting Data Dictionary API...")
    print(f"GitHub storage: {'enabled' if settings.use_github else 'disabled (using local file)'}")
    print(f"Cache refresh interval: {settings.cache_refresh_interval}s")
    
    # Load initial dictionary
    await cache.load_initial()
    
    # Start background refresh if using GitHub
    if settings.use_github:
        await cache.start_background_refresh()
        print("Background refresh started")
    
    yield
    
    # Shutdown
    print("Shutting down...")
    await cache.stop_background_refresh()
    await cache.save_to_local()  # Always save to local on shutdown
    print("Dictionary saved to local file")


# Create FastAPI app
app = FastAPI(
    title="Data Dictionary API",
    description="""
API for managing a centralized data dictionary with fuzzy matching capabilities.

## Features
- **Fuzzy Matching**: Find preferred/canonical names for variable terms
- **CRUD Operations**: Create, read, update, delete dictionary entries
- **Alias Management**: Add or remove aliases for entries
- **GitHub Sync**: Automatic sync across multiple server replicas
- **In-Memory Cache**: Fast lookups with background refresh

## Use Case
Standardize variable naming across teams:
- `accountNumber`, `acc_num`, `acnt_num` → all map to `account_number`
    """,
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(router)


@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint."""
    dictionary = cache.dictionary
    return {
        "status": "healthy",
        "entries_count": len(dictionary.entries) if dictionary else 0,
        "version": dictionary.version if dictionary else 0,
        "github_enabled": settings.use_github
    }


@app.get("/health", tags=["Health"])
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )
