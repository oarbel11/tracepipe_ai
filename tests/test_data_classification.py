import pytest
from scripts.data_classification.classifier import SensitiveDataClassifier, Classification
from scripts.data_classification.policy_propagator import PolicyPropagator
from scripts.data_classification.orchestrator import ClassificationOrchestrator

def test_classifier_detects_email():
    classifier = SensitiveDataClassifier()
    result = classifier.classify_column(
        "user_email",
        ["test@example.com", "user@domain.org"],
        "string"
    )
    assert result.column_name == "user_email"
    assert result.sensitivity_level.value in ["confidential", "restricted"]
    assert result.confidence > 0.7

def test_classifier_detects_ssn():
    classifier = SensitiveDataClassifier()
    result = classifier.classify_column(
        "ssn",
        ["123-45-6789", "987-65-4321"],
        "string"
    )
    assert "ssn" in result.detected_patterns
    assert result.sensitivity_level.value == "restricted"

def test_policy_propagator_traverses_lineage():
    lineage = {
        "table_a": ["table_b", "table_c"],
        "table_b": ["table_d"],
        "table_c": []
    }
    propagator = PolicyPropagator(lineage)
    downstream = propagator._traverse_downstream("table_a")
    assert "table_a" in downstream
    assert "table_b" in downstream
    assert "table_d" in downstream

def test_orchestrator_end_to_end():
    lineage = {"source_table": ["dest_table"]}
    orchestrator = ClassificationOrchestrator(lineage)
    columns = {
        "email": {"sample_values": ["test@example.com"], "data_type": "string"},
        "name": {"sample_values": ["John Doe"], "data_type": "string"}
    }
    policies = orchestrator.classify_and_propagate("source_table", columns)
    assert len(policies) > 0
    report = orchestrator.generate_compliance_report()
    assert report['total_columns'] == 2
    assert report['sensitive_columns'] >= 1
