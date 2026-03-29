from flask import Flask, jsonify, request
from datetime import datetime
from tracepipe_ai.lineage_history import LineageHistoryStorage

app = Flask(__name__)
storage = LineageHistoryStorage()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


@app.route("/api/lineage/store", methods=["POST"])
def store_lineage():
    """Store a lineage record."""
    data = request.get_json()
    if not data or "source" not in data or "target" not in data:
        return jsonify({"error": "Missing source or target"}), 400
    
    lineage_id = storage.store_lineage(
        source=data["source"],
        target=data["target"],
        metadata=data.get("metadata", {})
    )
    return jsonify({"id": lineage_id}), 201


@app.route("/api/lineage/query", methods=["GET"])
def query_lineage():
    """Query lineage history."""
    table = request.args.get("table")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    
    start_dt = datetime.fromisoformat(start_date) if start_date else None
    end_dt = datetime.fromisoformat(end_date) if end_date else None
    
    results = storage.query_lineage(
        table=table,
        start_date=start_dt,
        end_date=end_dt
    )
    return jsonify({"lineage": results}), 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
