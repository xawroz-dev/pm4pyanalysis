// schema.groovy

def defineSchema(graph) {
    def mgmt = graph.openManagement()
    
    // Property Keys
    if (!mgmt.containsPropertyKey('usecase')) mgmt.makePropertyKey('usecase').dataType(String.class).make()
    if (!mgmt.containsPropertyKey('eventId')) mgmt.makePropertyKey('eventId').dataType(String.class).make()
    if (!mgmt.containsPropertyKey('timestamp')) mgmt.makePropertyKey('timestamp').dataType(Long.class).make()
    if (!mgmt.containsPropertyKey('activityName')) mgmt.makePropertyKey('activityName').dataType(String.class).make()
    if (!mgmt.containsPropertyKey('appName')) mgmt.makePropertyKey('appName').dataType(String.class).make()
    if (!mgmt.containsPropertyKey('correlationValue')) mgmt.makePropertyKey('correlationValue').dataType(String.class).make()
    if (!mgmt.containsPropertyKey('journeyId')) mgmt.makePropertyKey('journeyId').dataType(String.class).make()
    if (!mgmt.containsPropertyKey('status')) mgmt.makePropertyKey('status').dataType(String.class).make()

    // Vertex Labels
    if (!mgmt.containsVertexLabel('Event')) mgmt.makeVertexLabel('Event').make()
    if (!mgmt.containsVertexLabel('CorrelationKey')) mgmt.makeVertexLabel('CorrelationKey').make()
    if (!mgmt.containsVertexLabel('Journey')) mgmt.makeVertexLabel('Journey').make()
    if (!mgmt.containsVertexLabel('Activity')) mgmt.makeVertexLabel('Activity').make()

    // Edge Labels
    if (!mgmt.containsEdgeLabel('HAS_CORRELATION')) mgmt.makeEdgeLabel('HAS_CORRELATION').make()
    if (!mgmt.containsEdgeLabel('PART_OF_JOURNEY')) mgmt.makeEdgeLabel('PART_OF_JOURNEY').make()
    if (!mgmt.containsEdgeLabel('CONTAINS_EVENT')) mgmt.makeEdgeLabel('CONTAINS_EVENT').make()
    if (!mgmt.containsEdgeLabel('NEXT')) mgmt.makeEdgeLabel('NEXT').make()

    // Indices (Multi-tenant: always include usecase)
    
    // Event Lookup: usecase + eventId
    if (!mgmt.containsGraphIndex('byUsecaseEventId')) {
        def usecase = mgmt.getPropertyKey('usecase')
        def eventId = mgmt.getPropertyKey('eventId')
        def eventLabel = mgmt.getVertexLabel('Event')
        mgmt.buildIndex('byUsecaseEventId', Vertex.class).addKey(usecase).addKey(eventId).indexOnly(eventLabel).buildCompositeIndex()
    }

    // Correlation Lookup: usecase + correlationValue
    if (!mgmt.containsGraphIndex('byUsecaseCorrelationValue')) {
        def usecase = mgmt.getPropertyKey('usecase')
        def correlationValue = mgmt.getPropertyKey('correlationValue')
        def correlationLabel = mgmt.getVertexLabel('CorrelationKey')
        mgmt.buildIndex('byUsecaseCorrelationValue', Vertex.class).addKey(usecase).addKey(correlationValue).indexOnly(correlationLabel).buildCompositeIndex()
    }

    // Journey Lookup: usecase + journeyId
    if (!mgmt.containsGraphIndex('byUsecaseJourneyId')) {
        def usecase = mgmt.getPropertyKey('usecase')
        def journeyId = mgmt.getPropertyKey('journeyId')
        def journeyLabel = mgmt.getVertexLabel('Journey')
        mgmt.buildIndex('byUsecaseJourneyId', Vertex.class).addKey(usecase).addKey(journeyId).indexOnly(journeyLabel).buildCompositeIndex()
    }
    
    // General Journey Lookup by Usecase (for listing)
    // Note: This might have low cardinality if many journeys, but useful for "get all journeys for usecase"
    if (!mgmt.containsGraphIndex('byUsecaseJourney')) {
        def usecase = mgmt.getPropertyKey('usecase')
        def journeyLabel = mgmt.getVertexLabel('Journey')
        mgmt.buildIndex('byUsecaseJourney', Vertex.class).addKey(usecase).indexOnly(journeyLabel).buildCompositeIndex()
    }

    mgmt.commit()
}
