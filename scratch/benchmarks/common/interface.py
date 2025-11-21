from abc import ABC, abstractmethod

class JourneyManager(ABC):
    @abstractmethod
    def setup(self):
        """Initialize the database/graph and create schema/indexes."""
        pass

    @abstractmethod
    def clean(self):
        """Clean up data to start fresh."""
        pass

    @abstractmethod
    def ingest_batch(self, events_batch):
        """
        Ingest a batch of events.
        events_batch: list of dicts {'id': str, 'correlation_ids': list, 'payload': dict}
        """
        pass

    @abstractmethod
    def process_events(self):
        """
        Run the stitching logic:
        1. Identify new events.
        2. Cluster/Graph traversal.
        3. Merge/Link journeys.
        """
        pass

    @abstractmethod
    def get_journey(self, event_id):
        """
        Return journey details for validation.
        Returns: {'journey_id': str, 'events': list[str]} or None
        """
        pass
