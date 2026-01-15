// Empty sample script - required by Gremlin Server
// This file is loaded on startup to ensure script engine is initialized

def globals = [:]

globals << [hook : [
  onStartUp: { ctx ->
    ctx.logger.info("JanusGraph Gremlin Server started successfully")
  },
  onShutDown: { ctx ->
    ctx.logger.info("JanusGraph Gremlin Server shutting down")
  }
] as LifeCycleHook]

globals << [g : graph.traversal()]
