# Data Dictionary API

A Python REST API for maintaining a centralized data dictionary with fuzzy matching capabilities. Designed for standardizing variable naming across teams.

## Problem

Different teams often name the same concepts differently:

| Team A | Team B | Team C | Canonical |
|--------|--------|--------|-----------|
| accountNumber | acc_num | ACCT_NO | account_number |
| customerName | cust_name | CLIENT_NAME | customer_name |

This API provides fuzzy matching to suggest the preferred/canonical name.

## Features

- **Fuzzy Matching**: Find canonical names using multiple algorithms
- **CRUD Operations**: Create, read, update, delete dictionary entries
- **Alias Management**: Add or remove aliases for entries
- **GitHub Sync**: Automatic sync across multiple server replicas
- **In-Memory Cache**: Fast lookups with 10-second background refresh

## Quick Start

```bash
# Install dependencies
py -m pip install -r requirements.txt

# Run the API
py main.py

# Or with uvicorn directly
uvicorn main:app --reload --port 8000
```

API available at: http://localhost:8000

Interactive docs: http://localhost:8000/docs

## API Endpoints

### Matching
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/match/{query}` | Fuzzy match a term |

### Entries (CRUD)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/entries` | List all entries |
| GET | `/entries/{name}` | Get an entry |
| POST | `/entries` | Create entry |
| PUT | `/entries/{name}` | Update entry |
| DELETE | `/entries/{name}` | Delete entry |

### Aliases
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/entries/{name}/aliases` | Add alias |
| DELETE | `/entries/{name}/aliases/{alias}` | Remove alias |

### Cache
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/refresh` | Force cache refresh |

## Examples

### Fuzzy Match
```bash
curl http://localhost:8000/match/acc_num
```

Response:
```json
{
  "query": "acc_num",
  "matches": [
    {
      "preferred_name": "account_number",
      "score": 100.0,
      "description": "Unique identifier for a financial account",
      "aliases": ["accountNumber", "acc_num", "ACCT_NO"],
      "matched_on": "acc_num"
    }
  ]
}
```

### Create Entry
```bash
curl -X POST http://localhost:8000/entries \
  -H "Content-Type: application/json" \
  -d '{
    "preferred_name": "payment_amount",
    "description": "Dollar amount of a payment",
    "examples": ["100.00", "250.50"],
    "aliases": ["paymentAmount", "pay_amt", "PMT_AMT"]
  }'
```

### Add Alias
```bash
curl -X POST http://localhost:8000/entries/account_number/aliases \
  -H "Content-Type: application/json" \
  -d '{"alias": "acctNum"}'
```

## GitHub Sync

To enable GitHub-based storage for multi-replica sync:

```bash
# Set environment variables
export GITHUB_TOKEN=your_personal_access_token
export GITHUB_REPO=username/data-dictionary
export GITHUB_FILE_PATH=dictionary.json
export GITHUB_BRANCH=main
```

Or create a `.env` file:
```ini
GITHUB_TOKEN=your_token
GITHUB_REPO=username/data-dictionary
GITHUB_FILE_PATH=dictionary.json
GITHUB_BRANCH=main
CACHE_REFRESH_INTERVAL=10
```

Without GitHub config, the API uses local file storage.

## Running Tests

```bash
py -m pytest tests/ -v
```

## Project Structure

```
data-dictionary-api/
├── main.py              # FastAPI app entry point
├── api.py               # REST endpoints
├── models.py            # Pydantic models
├── matcher.py           # Fuzzy matching engine
├── cache.py             # In-memory cache
├── github_storage.py    # GitHub integration
├── config.py            # Configuration
├── dictionary.json      # Local dictionary file
├── requirements.txt     # Dependencies
└── tests/
    ├── test_matcher.py  # Matcher tests
    ├── test_cache.py    # Cache tests
    └── test_api.py      # API tests
```

## Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| GITHUB_TOKEN | "" | GitHub personal access token |
| GITHUB_REPO | "" | Repository name (user/repo) |
| GITHUB_FILE_PATH | dictionary.json | Path to dictionary in repo |
| GITHUB_BRANCH | main | Branch to use |
| CACHE_REFRESH_INTERVAL | 10 | Seconds between refreshes |
| API_HOST | 0.0.0.0 | API host |
| API_PORT | 8000 | API port |
