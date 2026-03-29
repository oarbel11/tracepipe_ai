"""Tests for Advanced Impact Analysis & Proactive Change Alerts."""
import pytest
from src.impact_analysis import ChangeSimulator, AlertSystem


@pytest.fixture
def lineage_graph():
    return {
        "sales_table": {
            "tables": ["revenue_table", "analytics_table"],
            "dashboards": ["sales_dashboard"],
            "models": ["forecast_model"]
        },
        "users_table": {
            "tables": ["user_analytics"],
            "dashboards": [],
            "models": []
        }
    }


@pytest.fixture
def simulator(lineage_graph):
    return ChangeSimulator(lineage_graph)


@pytest.fixture
def alert_system():
    return AlertSystem()


def test_simulate_column_removal(simulator):
    """Test simulating column removal impact."""
    changes = {"column_removed": "price"}
    result = simulator.simulate_schema_change("sales_table", changes)
    
    assert result["table"] == "sales_table"
    assert result["changes"] == changes
    assert len(result["affected_tables"]) == 2
    assert "revenue_table" in result["affected_tables"]
    assert result["risk_level"] == "medium"
    assert "timestamp" in result


def test_simulate_type_change(simulator):
    """Test simulating data type change."""
    changes = {"type_changed": {"amount": "string_to_decimal"}}
    result = simulator.simulate_schema_change("sales_table", changes)
    
    assert result["risk_level"] == "medium"
    assert len(result["affected_dashboards"]) == 1
    assert len(result["affected_models"]) == 1


def test_simulate_column_addition(simulator):
    """Test simulating column addition."""
    changes = {"column_added": "new_field"}
    result = simulator.simulate_schema_change("sales_table", changes)
    
    assert result["risk_level"] == "low"


def test_create_alert(alert_system, simulator):
    """Test alert creation from impact analysis."""
    changes = {"column_removed": "important_field"}
    impact = simulator.simulate_schema_change("sales_table", changes)
    
    alert = alert_system.create_alert(impact)
    
    assert alert["id"] == 1
    assert alert["table"] == "sales_table"
    assert alert["status"] == "pending"
    assert alert["risk_level"] in ["low", "medium", "high"]


def test_get_alerts(alert_system, simulator):
    """Test retrieving alerts."""
    impact = simulator.simulate_schema_change("sales_table", {"column_removed": "x"})
    alert_system.create_alert(impact)
    
    alerts = alert_system.get_alerts()
    assert len(alerts) == 1
    
    pending = alert_system.get_alerts(status="pending")
    assert len(pending) == 1


def test_subscribe_to_entity(alert_system):
    """Test subscribing to entity alerts."""
    alert_system.register_subscriber("sales_table", ["user@example.com"])
    assert "sales_table" in alert_system.subscribers
    assert alert_system.subscribers["sales_table"] == ["user@example.com"]
