"""
================================================================================
JanusGraph Process Mining Benchmark - Complete Modular Script
================================================================================

This script provides a complete, self-contained solution for:
1. Generating synthetic process mining events
2. Ingesting events into JanusGraph with correlation keys
3. Stitching events into journeys using server-side Groovy
4. Validating data integrity
5. Benchmarking performance

REQUIREMENTS:
- Python 3.8+
- gremlinpython>=3.5.0
- nest_asyncio (for Jupyter/async compatibility)
- JanusGraph server running at the specified URI (default: ws://localhost:8182/gremlin)

USAGE:
1. Modify the CONFIGURATION section below to set your parameters
2. Run: python janusgraph_benchmark.py

================================================================================
"""

import time
import uuid
import os
from typing import List, Dict, Any

# Third-party imports
try:
    import nest_asyncio
    nest_asyncio.apply()  # Fix for "event loop already running" errors
except ImportError:
    print("Warning: nest_asyncio not installed. Install with: pip install nest_asyncio")

from gremlin_python.driver.client import Client


# ==============================================================================
# CONFIGURATION - MODIFY THESE VALUES AS NEEDED
# ==============================================================================

# Connection Settings
GREMLIN_URI = os.environ.get('GREMLIN_URI', 'ws://localhost:8182/gremlin')

# Benchmark Parameters
USECASE = "benchmark_demo"      # Unique identifier for this benchmark run (multi-tenancy)
NUM_JOURNEYS = 1000              # Number of customer journeys to simulate
NUM_APPS = 3                     # Number of applications in the process (e.g., 3 = App_0, App_1, App_2)
EVENTS_PER_APP = 30              # Number of events generated per application per journey
BATCH_SIZE = 1000                # Number of events to send per batch (larger = faster but more memory)

# Derived values (calculated automatically)
TOTAL_EVENTS = NUM_JOURNEYS * NUM_APPS * EVENTS_PER_APP

# Output Settings
RESULTS_FILE = "benchmark_results.txt"
VERBOSE = True                   # Set to False to reduce console output


# ==============================================================================
# GROOVY SCRIPTS (Server-side processing for performance)
# ==============================================================================

# Schema Definition Script - Creates vertex labels, edge labels, and indices
SCHEMA_SCRIPT = """
def defineSchema(graph) {
    def mgmt = graph.openManagement()
    
    // Check if schema already exists
    if (mgmt.getVertexLabel('Event') != null) {
        mgmt.rollback()
        return "Schema already exists"
    }
    
    // Property Keys
    def usecaseProp = mgmt.makePropertyKey('usecase').dataType(String.class).make()
    def eventIdProp = mgmt.makePropertyKey('eventId').dataType(String.class).make()
    def timestampProp = mgmt.makePropertyKey('timestamp').dataType(Long.class).make()
    def activityNameProp = mgmt.makePropertyKey('activityName').dataType(String.class).make()
    def appNameProp = mgmt.makePropertyKey('appName').dataType(String.class).make()
    def correlationValueProp = mgmt.makePropertyKey('correlationValue').dataType(String.class).make()
    def journeyIdProp = mgmt.makePropertyKey('journeyId').dataType(String.class).make()
    def statusProp = mgmt.makePropertyKey('status').dataType(String.class).make()
    
    // Vertex Labels
    def eventLabel = mgmt.makeVertexLabel('Event').make()
    def corrKeyLabel = mgmt.makeVertexLabel('CorrelationKey').make()
    def journeyLabel = mgmt.makeVertexLabel('Journey').make()
    
    // Edge Labels
    mgmt.makeEdgeLabel('HAS_CORRELATION').make()
    mgmt.makeEdgeLabel('PART_OF_JOURNEY').make()
    
    // Composite Indices (for efficient lookups by usecase)
    mgmt.buildIndex('byUsecaseEventId', Vertex.class)
        .addKey(usecaseProp).addKey(eventIdProp)
        .indexOnly(eventLabel).buildCompositeIndex()
        
    mgmt.buildIndex('byUsecaseCorrelationValue', Vertex.class)
        .addKey(usecaseProp).addKey(correlationValueProp)
        .indexOnly(corrKeyLabel).buildCompositeIndex()
        
    mgmt.buildIndex('byUsecaseJourneyId', Vertex.class)
        .addKey(usecaseProp).addKey(journeyIdProp)
        .indexOnly(journeyLabel).buildCompositeIndex()
    
    mgmt.commit()
    return "Schema created successfully"
}
defineSchema(graph)
"""

# Optimized Batch Ingestion Script - Pre-creates correlation keys, then links events
BATCH_INGEST_SCRIPT = """
def ingestBatch(events, usecase, correlationKeys) {
    def g = graph.traversal()
    def keyCache = [:]
    
    // Phase 1: Pre-create all correlation keys first (idempotent using fold/coalesce)
    correlationKeys.each { key ->
        def v_corr = g.V().has('CorrelationKey', 'usecase', usecase)
            .has('correlationValue', key)
            .fold()
            .coalesce(
                __.unfold(), 
                __.addV('CorrelationKey')
                  .property('usecase', usecase)
                  .property('correlationValue', key)
            ).next()
        keyCache[key] = v_corr
    }
    
    // Phase 2: Add all events and link to cached correlation keys
    events.each { event ->
        def v_event = g.addV('Event')
            .property('usecase', usecase)
            .property('eventId', event.eventId)
            .property('timestamp', event.timestamp)
            .property('activityName', event.activityName)
            .property('appName', event.appName)
            .property('correlationValue', event.correlationValue)
            .next()
            
        def v_corr = keyCache[event.correlationValue]
        if (v_corr != null) {
            g.V(v_event).addE('HAS_CORRELATION').to(v_corr).iterate()
        }
    }
    graph.tx().commit()
    return "Batch processed"
}
ingestBatch(events, usecase, correlationKeys)
"""

# Stitching Script - Groups events into journeys based on shared correlation keys
STITCH_SCRIPT = """
def stitch(usecase) {
    def g = graph.traversal()
    def start = System.currentTimeMillis()
    def stitchedCount = 0
    
    // Get all CorrelationKeys for this usecase
    def correlationKeys = g.V().has('CorrelationKey', 'usecase', usecase).toList()
    
    correlationKeys.each { corrKey ->
        try {
            // Get all events connected to this correlation key
            def events = g.V(corrKey).in('HAS_CORRELATION').toList()
            if (events.isEmpty()) return
            
            // Find existing journeys connected to these events
            def existingJourneys = g.V(corrKey).in('HAS_CORRELATION').out('PART_OF_JOURNEY').dedup().toList()
            
            def journeyVertex
            
            if (existingJourneys.isEmpty()) {
                // Case A: No existing journey - create new one
                journeyVertex = g.addV('Journey')
                    .property('usecase', usecase)
                    .property('journeyId', UUID.randomUUID().toString())
                    .property('status', 'active')
                    .next()
                stitchedCount++
            } else if (existingJourneys.size() == 1) {
                // Case B: Single existing journey - use it
                journeyVertex = existingJourneys[0]
            } else {
                // Case C: Multiple journeys exist - merge into first one
                journeyVertex = existingJourneys[0]
                
                existingJourneys.drop(1).each { otherJourney ->
                    // Move events from other journeys to master journey
                    g.V(otherJourney).in('PART_OF_JOURNEY').each { event ->
                        if (!g.V(event).out('PART_OF_JOURNEY').where(__.is(journeyVertex)).hasNext()) {
                            g.V(event).addE('PART_OF_JOURNEY').to(journeyVertex).iterate()
                        }
                        g.V(event).outE('PART_OF_JOURNEY').where(__.inV().is(otherJourney)).drop().iterate()
                    }
                    // Delete merged journey
                    g.V(otherJourney).drop().iterate()
                }
            }
            
            // Link all events to the journey (idempotent)
            events.each { event ->
                if (!g.V(event).out('PART_OF_JOURNEY').hasNext()) {
                    g.V(event).addE('PART_OF_JOURNEY').to(journeyVertex).iterate()
                }
            }
            
            graph.tx().commit()
            
        } catch (Exception e) {
            graph.tx().rollback()
        }
    }
    
    def end = System.currentTimeMillis()
    return "Stitching completed for usecase " + usecase + ". Created " + stitchedCount + " journeys. Time: " + (end - start) + " ms"
}
stitch(usecase)
"""


# ==============================================================================
# DATA GENERATION
# ==============================================================================

class EventGenerator:
    """
    Generates synthetic process mining events for testing.
    
    The generator creates a chain of applications where each app shares
    correlation keys with its neighbors, allowing journey stitching:
    
    Journey 1: App_0 --[corr_0_1_0]--> App_1 --[corr_1_2_0]--> App_2
    Journey 2: App_0 --[corr_0_1_1]--> App_1 --[corr_1_2_1]--> App_2
    ...
    """
    
    def __init__(self, num_journeys: int, num_apps: int, events_per_app: int, usecase: str):
        self.num_journeys = num_journeys
        self.num_apps = num_apps
        self.events_per_app = events_per_app
        self.usecase = usecase
        self.apps = [f"App_{i}" for i in range(num_apps)]
        
    def generate_all(self) -> List[Dict[str, Any]]:
        """
        Generate all events for all journeys.
        
        Returns:
            List of event dictionaries with eventId, timestamp, activityName, 
            appName, and correlationValue fields.
        """
        all_events = []
        
        for journey_idx in range(self.num_journeys):
            # Generate correlation keys that chain applications together
            # corr_0_1_X links App_0 and App_1
            # corr_1_2_X links App_1 and App_2
            keys = []
            for k in range(self.num_apps - 1):
                keys.append(f"corr_{k}_{k+1}_{journey_idx}")
            
            # Handle single-app case
            if not keys:
                keys.append(f"corr_self_{journey_idx}")

            for app_idx, app_name in enumerate(self.apps):
                # Determine which correlation keys this app uses
                # App_0 uses keys[0]
                # App_1 uses keys[0] AND keys[1] (bridge between App_0 and App_2)
                # App_2 uses keys[1]
                app_keys = []
                if app_idx < len(keys):
                    app_keys.append(keys[app_idx])
                if app_idx > 0 and (app_idx - 1) < len(keys):
                    app_keys.append(keys[app_idx - 1])
                
                app_keys = list(set(app_keys))  # Remove duplicates
                
                # Generate events for this app
                for event_num in range(self.events_per_app):
                    correlation_value = app_keys[event_num % len(app_keys)]
                    
                    all_events.append({
                        "eventId": str(uuid.uuid4()),
                        "timestamp": int(time.time() * 1000) + event_num,
                        "activityName": f"Activity_{app_name}_{event_num}",
                        "appName": app_name,
                        "correlationValue": correlation_value
                    })
                    
        return all_events


# ==============================================================================
# CORE FUNCTIONS
# ==============================================================================

def log(message: str):
    """Print message if VERBOSE is True."""
    if VERBOSE:
        print(message)


def wait_for_connection(client: Client, retries: int = 20, delay: int = 5) -> bool:
    """
    Wait for JanusGraph to become available.
    
    Args:
        client: Gremlin client instance
        retries: Number of connection attempts
        delay: Seconds to wait between attempts
        
    Returns:
        True if connected, False otherwise
    """
    for i in range(retries):
        try:
            client.submit("g.V().limit(1)").all().result()
            log("✓ Connected to JanusGraph")
            return True
        except Exception as e:
            log(f"Waiting for JanusGraph... ({i+1}/{retries}) - {e}")
            time.sleep(delay)
    return False


def ensure_schema(client: Client) -> bool:
    """
    Create the graph schema if it doesn't exist.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        result = client.submit(SCHEMA_SCRIPT).all().result()
        log(f"✓ Schema: {result}")
        return True
    except Exception as e:
        log(f"✗ Schema error: {e}")
        return False


def cleanup_usecase(client: Client, usecase: str):
    """
    Remove all vertices for a specific usecase.
    Useful for running clean benchmarks.
    """
    try:
        client.submit(f"g.V().has('usecase', '{usecase}').drop()").all().result()
        log(f"✓ Cleaned up usecase: {usecase}")
    except Exception as e:
        log(f"✗ Cleanup error: {e}")


def ingest_events(client: Client, events: List[Dict], usecase: str, batch_size: int) -> float:
    """
    Ingest events into JanusGraph in batches.
    
    Args:
        client: Gremlin client
        events: List of event dictionaries
        usecase: Usecase identifier
        batch_size: Number of events per batch
        
    Returns:
        Duration in seconds
    """
    total_batches = (len(events) + batch_size - 1) // batch_size
    log(f"Ingesting {len(events)} events in {total_batches} batches of {batch_size}...")
    
    start_time = time.time()
    
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        
        # Extract unique correlation keys for this batch
        correlation_keys = list(set(e["correlationValue"] for e in batch))
        
        try:
            bindings = {
                'events': batch,
                'usecase': usecase,
                'correlationKeys': correlation_keys
            }
            client.submit(BATCH_INGEST_SCRIPT, bindings).all().result()
            
            batch_num = i // batch_size + 1
            if batch_num % 10 == 0 or batch_num == total_batches:
                log(f"  Processed batch {batch_num}/{total_batches}")
                
        except Exception as e:
            log(f"✗ Error in batch {i}: {e}")
            
    duration = time.time() - start_time
    throughput = len(events) / duration
    log(f"✓ Ingestion completed in {duration:.2f}s ({throughput:.0f} events/s)")
    
    return duration


def stitch_journeys(client: Client, usecase: str) -> float:
    """
    Run server-side stitching to group events into journeys.
    
    Args:
        client: Gremlin client
        usecase: Usecase identifier
        
    Returns:
        Duration in seconds
    """
    log("Running stitching...")
    start_time = time.time()
    
    try:
        bindings = {'usecase': usecase}
        result = client.submit(STITCH_SCRIPT, bindings).all().result()
        log(f"  {result}")
    except Exception as e:
        log(f"✗ Stitching error: {e}")
        
    duration = time.time() - start_time
    log(f"✓ Stitching completed in {duration:.2f}s")
    
    return duration


def validate(client: Client, usecase: str) -> Dict[str, Any]:
    """
    Validate the ingested and stitched data.
    
    Args:
        client: Gremlin client
        usecase: Usecase identifier
        
    Returns:
        Dictionary with validation results
    """
    log("Validating data...")
    results = {}
    
    try:
        # Count events
        event_count = client.submit(
            f"g.V().has('Event', 'usecase', '{usecase}').count()"
        ).all().result()[0]
        results['events'] = event_count
        log(f"  Total Events: {event_count}")
        
        # Count journeys
        journey_count = client.submit(
            f"g.V().has('Journey', 'usecase', '{usecase}').count()"
        ).all().result()[0]
        results['journeys'] = journey_count
        log(f"  Total Journeys: {journey_count}")
        
        # Check for orphaned events (events without a journey)
        orphan_count = client.submit(
            f"g.V().has('Event', 'usecase', '{usecase}').where(__.not(__.out('PART_OF_JOURNEY'))).count()"
        ).all().result()[0]
        results['orphans'] = orphan_count
        log(f"  Orphaned Events: {orphan_count}")
        
        if orphan_count == 0 and journey_count > 0:
            log("✓ Validation PASSED: All events stitched to journeys")
            results['status'] = 'PASSED'
        else:
            log("✗ Validation FAILED")
            results['status'] = 'FAILED'
            
    except Exception as e:
        log(f"✗ Validation error: {e}")
        results['status'] = 'ERROR'
        results['error'] = str(e)
        
    return results


# ==============================================================================
# MAIN BENCHMARK FUNCTION
# ==============================================================================

def run_benchmark():
    """
    Execute the full benchmark: generate, ingest, stitch, and validate.
    """
    print("=" * 70)
    print("JanusGraph Process Mining Benchmark")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  URI:            {GREMLIN_URI}")
    print(f"  Usecase:        {USECASE}")
    print(f"  Journeys:       {NUM_JOURNEYS:,}")
    print(f"  Apps:           {NUM_APPS}")
    print(f"  Events/App:     {EVENTS_PER_APP}")
    print(f"  Batch Size:     {BATCH_SIZE:,}")
    print(f"  Total Events:   {TOTAL_EVENTS:,}")
    print("=" * 70)
    
    # Create client
    client = Client(GREMLIN_URI, 'g')
    
    try:
        # Step 1: Wait for connection
        if not wait_for_connection(client):
            print("ERROR: Could not connect to JanusGraph. Exiting.")
            return
        
        # Step 2: Ensure schema exists
        ensure_schema(client)
        
        # Step 3: Clean up previous runs
        cleanup_usecase(client, USECASE)
        
        # Step 4: Generate events
        log(f"\nGenerating {TOTAL_EVENTS:,} events...")
        generator = EventGenerator(NUM_JOURNEYS, NUM_APPS, EVENTS_PER_APP, USECASE)
        events = generator.generate_all()
        log(f"✓ Generated {len(events):,} events")
        
        # Step 5: Ingest
        log("\n--- INGESTION ---")
        ingest_time = ingest_events(client, events, USECASE, BATCH_SIZE)
        
        # Step 6: Stitch
        log("\n--- STITCHING ---")
        stitch_time = stitch_journeys(client, USECASE)
        
        # Step 7: Validate
        log("\n--- VALIDATION ---")
        validation = validate(client, USECASE)
        
        # Step 8: Summary
        total_time = ingest_time + stitch_time
        print("\n" + "=" * 70)
        print("BENCHMARK RESULTS")
        print("=" * 70)
        print(f"  Ingestion Time:  {ingest_time:.2f}s")
        print(f"  Stitching Time:  {stitch_time:.2f}s")
        print(f"  Total Time:      {total_time:.2f}s")
        print(f"  Throughput:      {TOTAL_EVENTS / ingest_time:.0f} events/s")
        print(f"  Events:          {validation.get('events', 'N/A'):,}")
        print(f"  Journeys:        {validation.get('journeys', 'N/A'):,}")
        print(f"  Validation:      {validation.get('status', 'N/A')}")
        print("=" * 70)
        
        # Save results to file
        with open(RESULTS_FILE, "a") as f:
            f.write(f"Usecase: {USECASE}, Journeys: {NUM_JOURNEYS}, Events: {TOTAL_EVENTS}, ")
            f.write(f"Ingest: {ingest_time:.2f}s, Stitch: {stitch_time:.2f}s, Total: {total_time:.2f}s\n")
        log(f"\nResults saved to {RESULTS_FILE}")
        
    finally:
        client.close()


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    run_benchmark()
