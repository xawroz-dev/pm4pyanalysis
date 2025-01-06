# app.py

from flask import Flask, request, jsonify
from storage import GlobalStorage
from services import ProcessMiningService

app = Flask(__name__)

storage = GlobalStorage()
service = ProcessMiningService(storage)

@app.route("/api/variants", methods=["POST"])
def import_variants():
    """
    POST /api/variants
    {
       "file_path": "output.xes",
       "log_id": "my_log_1"
    }
    -> Imports XES, generates named variants, stores in memory.
    Returns a list of variant names, e.g. ["Variant_1", "Variant_2", ...]
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON payload"}), 400

    file_path = data.get("file_path")
    log_id = data.get("log_id")
    if not file_path or not log_id:
        return jsonify({"error": "file_path and log_id are required"}), 400

    try:
        service.import_log_and_generate_variants(file_path, log_id)
        variant_dict = storage.get_variants(log_id)
        return jsonify({
            "message": f"Log '{log_id}' imported, variants generated.",
            "variants": list(variant_dict.keys())
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/variants/add", methods=["POST"])
def add_variant():
    """
    POST /api/variants/add
    {
      "log_id": "my_log_1",
      "variant_name": "Variant_999",
      "activity_sequence": ["A","B","C"]
    }
    -> Adds a custom variant with that activity sequence
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON payload"}), 400

    log_id = data.get("log_id")
    variant_name = data.get("variant_name")
    activity_sequence = data.get("activity_sequence", [])

    if not log_id or not variant_name or not activity_sequence:
        return jsonify({"error": "log_id, variant_name, and activity_sequence are required"}), 400

    try:
        service.add_custom_variant(log_id, variant_name, activity_sequence)
        return jsonify({"message": f"Custom variant '{variant_name}' added to log '{log_id}'"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/discover", methods=["POST"])
def discover_model():
    """
    POST /api/discover
    {
      "log_id": "my_log_1",
      "variant_names": ["Variant_1","Variant_999"],
      "model_id": "model_1"
    }
    -> Discovers a Petri net from the chosen variants, stores it under 'model_1', returns PNML + metrics
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON data"}), 400

    log_id = data.get("log_id")
    variant_names = data.get("variant_names", [])
    model_id = data.get("model_id")
    if not log_id or not variant_names or not model_id:
        return jsonify({"error": "log_id, variant_names, and model_id are required"}), 400

    try:
        model_data = service.discover_process_from_variants(log_id, variant_names, model_id)
        return jsonify({
            "model_id": model_id,
            "pnml": model_data["pnml"],
            "metrics": model_data["metrics"],
            "variants_used": model_data["variants_used"]
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fitness", methods=["POST"])
def fitness_check():
    """
    POST /api/fitness
    {
      "model_id": "model_1",
      "log_id": "my_log_1",
      "variant_names": ["Variant_1"]
    }
    -> alignment-based fitness check
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON data"}), 400

    model_id = data.get("model_id")
    log_id = data.get("log_id")
    variant_names = data.get("variant_names", [])
    if not model_id or not log_id or not variant_names:
        return jsonify({"error": "model_id, log_id, and variant_names are required"}), 400

    try:
        fitness_result = service.check_fitness_only(model_id, log_id, variant_names)
        return jsonify(fitness_result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# app.py (excerpt)
@app.route("/api/snapshots", methods=["POST"])
def create_snapshot():
    """
    POST /api/snapshots
    {
      "snapshot_id": "snap_001",
      "model_id": "model_1",
      "log_id": "my_log_1",
      "variant_names": ["Variant_1","Variant_2"]
    }
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON payload"}), 400

    snapshot_id = data.get("snapshot_id")
    model_id = data.get("model_id")
    log_id = data.get("log_id")
    variant_names = data.get("variant_names", [])

    if not snapshot_id or not model_id or not log_id:
        return jsonify({"error": "snapshot_id, model_id, and log_id are required"}), 400

    try:
        snapshot_data = service.create_snapshot(snapshot_id, model_id, log_id, variant_names)
        return jsonify(snapshot_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/snapshots/<snapshot_id>", methods=["GET"])
def get_snapshot(snapshot_id):
    """
    GET /api/snapshots/<snapshot_id>
    -> returns the stored snapshot, or 404 if not found
    """
    snap = storage.get_snapshot(snapshot_id)
    if not snap:
        return jsonify({"error": f"Snapshot '{snapshot_id}' not found"}), 404
    return jsonify(snap), 200


@app.route("/api/petrinet-to-bpmn", methods=["POST"])
def petrinet_to_bpmn():
    """
    POST /api/petrinet-to-bpmn
    JSON body: { "model_id": "model_123" }
    Returns: { "bpmn": "<bpmn_xml_string>" }
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON payload"}), 400

    model_id = data.get("model_id")
    if not model_id:
        return jsonify({"error": "model_id is required"}), 400

    # Retrieve the Petri net from storage
    model_data = storage.get_model(model_id)
    if not model_data:
        return jsonify({"error": f"Model '{model_id}' not found."}), 404

    net = model_data.get("net")
    im = model_data.get("im")
    fm = model_data.get("fm")
    if not net or not im or not fm:
        return jsonify({"error": "Invalid Petri net data in model storage"}), 500

    try:
        # Convert to BPMN string
        bpmn_str = service.petrinet_to_bpmn_string(net, im, fm)
        return jsonify({"bpmn": bpmn_str}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/filter-log", methods=["POST"])
def filter_log():
    """
    POST /api/filter-log
    Request JSON example:
    {
      "log_id": "my_log_1",
      "start_activities": ["A","B"],
      "end_activities": ["C"],
      "remove_activities": ["X","Y"],
      "directly_follows": [["A","C"]],  # keep traces that have A->C
      "eventually_follows": [["A","Z"]],
      "event_attributes": {"org:resource": "john"}
    }
    The method filters the log by these parameters and overwrites it in storage.
    Returns how many traces remain, etc.
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON body"}), 400

    log_id = data.get("log_id")
    if not log_id:
        return jsonify({"error": "log_id is required"}), 400

    try:
        start_activities = data.get("start_activities")
        end_activities = data.get("end_activities")
        remove_activities = data.get("remove_activities")
        directly_follows = data.get("directly_follows")
        eventually_follows = data.get("eventually_follows")
        event_attributes = data.get("event_attributes")

        # Call the service method
        result = service.filter_log(
            log_id=log_id,
            start_activities=start_activities,
            end_activities=end_activities,
            remove_activities=remove_activities,
            directly_follows=directly_follows,
            eventually_follows=eventually_follows,
            event_attributes=event_attributes
        )
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
