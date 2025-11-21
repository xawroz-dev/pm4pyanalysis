# Apache AGE Event Journey System

This project demonstrates a scalable event journey tracking system using **Apache AGE** (Graph extension for PostgreSQL) and **Python**.

## Features
- **Scalable Ingestion**: Events are ingested with "NEW" status and processed incrementally.
- **Graph Correlation**: Events are linked based on shared `correlation_ids`.
- **Dynamic Merging**: If a new event bridges two previously separate journeys, they are automatically merged into the oldest journey.
- **Data Cleanup**: Includes tools to reset data for testing.

## Prerequisites
- Docker & Docker Compose
- Python 3.8+

## Setup

1. **Start the Database**
   ```bash
   docker-compose up -d
   ```
   This starts a PostgreSQL 16 container with Apache AGE installed.

2. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Demo

Run the main script to see the full lifecycle (Ingest -> Process -> Merge -> Verify):

```bash
python main.py
```

## Project Structure

- `journey_manager.py`: Contains the core logic for graph management, event ingestion, and journey processing.
- `main.py`: A demonstration script that simulates a real-world scenario with disjoint clusters merging.
- `docker-compose.yml`: Infrastructure definition.

## Key Logic (`journey_manager.py`)

The `process_new_events` method implements the intelligent merging logic:
1. Finds all `NEW` events.
2. Queries the graph for existing neighbors (via correlation IDs).
3. If multiple existing journeys are found connected to the new event, it identifies the "Oldest" journey.
4. It moves all events from the newer journeys to the oldest one and deletes the empty journey nodes.
5. Finally, it links the new event to the winner journey.
