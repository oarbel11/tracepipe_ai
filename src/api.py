"""API routes for TracePipe AI."""
from flask import Flask, jsonify, request
from src.impact_analysis import ChangeSimulator, AlertSystem

app = Flask(__name__)
simulator = ChangeSimulator()
alert_system = AlertSystem()


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"})


@app.route('/api/lineage/graph', methods=['POST'])
def set_lineage_graph():
    """Set the lineage graph for impact analysis."""
    data = request.get_json()
    global simulator
    simulator = ChangeSimulator(data.get('graph', {}))
    return jsonify({"status": "success"})


@app.route('/api/impact/simulate', methods=['POST'])
def simulate_change():
    """Simulate a change and analyze impact."""
    data = request.get_json()
    table_name = data.get('table')
    changes = data.get('changes', {})
    
    if not table_name:
        return jsonify({"error": "table name required"}), 400
    
    impact = simulator.simulate_schema_change(table_name, changes)
    
    if impact['risk_level'] in ['high', 'medium']:
        alert_system.create_alert(impact)
    
    return jsonify(impact)


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Get all alerts."""
    status = request.args.get('status')
    alerts = alert_system.get_alerts(status)
    return jsonify({"alerts": alerts})


@app.route('/api/alerts/subscribe', methods=['POST'])
def subscribe():
    """Subscribe to alerts for an entity."""
    data = request.get_json()
    entity = data.get('entity')
    contacts = data.get('contacts', [])
    
    if not entity:
        return jsonify({"error": "entity required"}), 400
    
    alert_system.register_subscriber(entity, contacts)
    return jsonify({"status": "subscribed", "entity": entity})


if __name__ == '__main__':
    app.run(debug=True)
