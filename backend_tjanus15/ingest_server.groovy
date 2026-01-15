// ingest_server.groovy

def ingestData(graph) {
    def g = graph.traversal()
    def start = System.currentTimeMillis()
    
    def numJourneys = 1000
    def eventsPerApp = 30
    def apps = ['AppA', 'AppB', 'AppC']
    
    // Use a transaction batch size
    def batchSize = 1000
    def counter = 0
    
    (0..<numJourneys).each { j_idx ->
        def key_ab = "corr_ab_${j_idx}"
        def key_bc = "corr_bc_${j_idx}"
        
        // AppA
        (0..<eventsPerApp).each { i ->
            g.addV('Event')
             .property('eventId', UUID.randomUUID().toString())
             .property('timestamp', System.currentTimeMillis() + i)
             .property('activityName', "Activity_A_${i}")
             .property('appName', "AppA")
             .property('correlationValue', key_ab)
             .as('e')
             .coalesce(
                 __.V().has('CorrelationKey', 'correlationValue', key_ab),
                 __.addV('CorrelationKey').property('correlationValue', key_ab)
             ).as('c')
             .addE('HAS_CORRELATION').from('e').to('c')
             .iterate()
             
             counter++
             if (counter % batchSize == 0) graph.tx().commit()
        }
        
        // AppB
        (0..<eventsPerApp).each { i ->
            def c_val = (i % 2 == 0) ? key_ab : key_bc
            g.addV('Event')
             .property('eventId', UUID.randomUUID().toString())
             .property('timestamp', System.currentTimeMillis() + i)
             .property('activityName', "Activity_B_${i}")
             .property('appName', "AppB")
             .property('correlationValue', c_val)
             .as('e')
             .coalesce(
                 __.V().has('CorrelationKey', 'correlationValue', c_val),
                 __.addV('CorrelationKey').property('correlationValue', c_val)
             ).as('c')
             .addE('HAS_CORRELATION').from('e').to('c')
             .iterate()
             
             counter++
             if (counter % batchSize == 0) graph.tx().commit()
        }
        
        // AppC
        (0..<eventsPerApp).each { i ->
            g.addV('Event')
             .property('eventId', UUID.randomUUID().toString())
             .property('timestamp', System.currentTimeMillis() + i)
             .property('activityName', "Activity_C_${i}")
             .property('appName', "AppC")
             .property('correlationValue', key_bc)
             .as('e')
             .coalesce(
                 __.V().has('CorrelationKey', 'correlationValue', key_bc),
                 __.addV('CorrelationKey').property('correlationValue', key_bc)
             ).as('c')
             .addE('HAS_CORRELATION').from('e').to('c')
             .iterate()
             
             counter++
             if (counter % batchSize == 0) graph.tx().commit()
        }
    }
    
    graph.tx().commit()
    
    def end = System.currentTimeMillis()
    return "Ingested ${counter} events in ${end - start} ms"
}
