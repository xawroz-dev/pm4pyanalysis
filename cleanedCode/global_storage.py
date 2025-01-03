# storage.py

class GlobalStorage:
    """
    In-memory storage for:
      - logs[log_id]: a PM4Py EventLog
      - variants[log_id]: dict of { variant_name: [Traces] }
      - models[model_id]: dict { net, im, fm, pnml, metrics, ... }
      - snapshots[snapshot_id]: your final JSON snapshot
    """

    def __init__(self):
        self.logs = {}
        self.variants = {}
        self.models = {}
        self.snapshots = {}
        self.process_tree = {}
        self.named_varaints = {}
        self.named_collapsed_variants = {}

    def save_log(self, log_id, log_obj):
        self.logs[log_id] = log_obj

    def get_log(self, log_id):
        return self.logs.get(log_id)

    def save_process_tree(self, log_id, process_tree):
        self.process_tree[log_id] = process_tree

    def get_process_tree(self, log_id):
        return self.process_tree.get(log_id)

    def save_variants(self, log_id, variants_dict):
        """
        variants_dict example:
            { "Variant_1": [Trace, Trace, ...],
              "Variant_2": [Trace, ...]
            }
        """
        self.variants[log_id] = variants_dict

    def get_variants(self, log_id):
        return self.variants.get(log_id, {})


    def save_named_variants(self, log_id, variants_dict):
        """
        variants_dict example:
            { "Variant_1": [Trace, Trace, ...],
              "Variant_2": [Trace, ...]
            }
        """
        self.named_varaints[log_id] = variants_dict

    def get_named_variants(self, log_id):
        return self.named_varaints.get(log_id, {})


    def save_named_collapsed_variants(self, log_id, variants_dict):
        """
        variants_dict example:
            { "Variant_1": [Trace, Trace, ...],
              "Variant_2": [Trace, ...]
            }
        """
        self.named_collapsed_variants[log_id] = variants_dict

    def get_named_collapsed_variants(self, log_id):
        return self.named_varaints.get(log_id, {})

    def save_model(self, model_id, model_data):
        """
        model_data example:
            {
               "net": <PetriNet>,
               "im": <Marking>,
               "fm": <Marking>,
               "pnml": <str>,
               "metrics": { "fitness":..., "soundness":..., },
               "variants_used": [ "Variant_1", ... ]
            }
        """
        self.models[model_id] = model_data

    def get_model(self, model_id):
        return self.models.get(model_id)

    def save_snapshot(self, snapshot_id, snapshot_data):
        """
        snapshot_data example:
            {"snapshot": {... your JSON structure ...}}
        """
        self.snapshots[snapshot_id] = snapshot_data

    def get_snapshot(self, snapshot_id):
        return self.snapshots.get(snapshot_id)
