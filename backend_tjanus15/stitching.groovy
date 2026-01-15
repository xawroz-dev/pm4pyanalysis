// stitching.groovy

def stitch(usecase) {
    def g = graph.traversal()
    def start = System.currentTimeMillis()
    def stitchedCount = 0
    
    // Iterate all CorrelationKeys for the usecase
    // In production, this should be driven by a queue or a "dirty" flag.
    // For now, we scan all.
    
    def correlationKeys = g.V().has('CorrelationKey', 'usecase', usecase).toList()
    
    correlationKeys.each { corrKey ->
        try {
            // Get all connected events (that are not yet fully processed? No, we check all)
            def events = g.V(corrKey).in('HAS_CORRELATION').toList()
            if (events.isEmpty()) return
            
            // Find existing journeys connected to these events
            def existingJourneys = g.V(corrKey).in('HAS_CORRELATION').out('PART_OF_JOURNEY').dedup().toList()
            
            def journeyVertex
            
            if (existingJourneys.isEmpty()) {
                // Case A: Create new Journey
                journeyVertex = g.addV('Journey')
                    .property('usecase', usecase)
                    .property('journeyId', UUID.randomUUID().toString())
                    .property('status', 'active')
                    .next()
                stitchedCount++
            } else if (existingJourneys.size() == 1) {
                // Case B: Use existing
                journeyVertex = existingJourneys[0]
            } else {
                // Case C: Merge
                // Pick oldest (by creation time? or just random for now as we don't have creation time on Journey yet)
                // Let's assume first one is "master"
                journeyVertex = existingJourneys[0]
                
                existingJourneys.drop(1).each { otherJourney ->
                    // Move events
                    g.V(otherJourney).in('PART_OF_JOURNEY').each { event ->
                         // Idempotent edge move
                         if (!g.V(event).out('PART_OF_JOURNEY').where(__.is(journeyVertex)).hasNext()) {
                             g.V(event).addE('PART_OF_JOURNEY').to(journeyVertex).iterate()
                         }
                         // Drop old edge
                         g.V(event).outE('PART_OF_JOURNEY').where(__.inV().is(otherJourney)).drop().iterate()
                    }
                    // Delete other journey
                    g.V(otherJourney).drop().iterate()
                }
            }
            
            // Link all events to the master journey
            events.each { event ->
                if (!g.V(event).out('PART_OF_JOURNEY').hasNext()) {
                    g.V(event).addE('PART_OF_JOURNEY').to(journeyVertex).iterate()
                } else {
                    // If it has a journey, make sure it's the right one (it should be, due to merge logic above)
                    // But if it points to a different one (race condition?), we might need to fix.
                    // For now, assume single threaded per usecase or consistent.
                }
            }
            
            // Commit per key or batch? 
            // Per key is safer for concurrency but slower.
            // Let's commit every 10 keys?
            // For now, commit per key to be safe.
            graph.tx().commit()
            
        } catch (Exception e) {
            graph.tx().rollback()
            // Log error but continue
            // println "Error processing key ${corrKey}: ${e.message}"
        }
    }
    
    def end = System.currentTimeMillis()
    return "Stitching completed for usecase ${usecase}. Created/Merged journeys. Time: ${end - start} ms".toString()
}
