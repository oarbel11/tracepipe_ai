import pytest
from scripts.data_classification.classifier import SensitiveDataClassifier, Classification
from scripts.data_classification.policy_propagator import PolicyPropagator

def test_classifier_detects_pii():
    classifier = SensitiveDataClassifier()
    results = classifier.classify_column('users', 'email')
    assert len(results) == 1
    assert results[0].classification_type == 'email'
    assert 'PII' in results[0].policy_tags

def test_classifier_detects_sensitive_data():
    classifier = SensitiveDataClassifier()
    results = classifier.classify_column('patients', 'diagnosis')
    assert len(results) == 1
    assert results[0].classification_type == 'medical'
    assert 'PHI' in results[0].policy_tags
    assert 'sensitive' in results[0].policy_tags

def test_classify_table():
    classifier = SensitiveDataClassifier()
    columns = ['customer_id', 'email', 'phone', 'address']
    results = classifier.classify_table('customers', columns)
    assert len(results) >= 3
    types = [r.classification_type for r in results]
    assert 'email' in types
    assert 'phone' in types

def test_classify_catalog():
    classifier = SensitiveDataClassifier()
    schema = {'users': ['email', 'name'], 'orders': ['credit_card']}
    results = classifier.classify_catalog('test_catalog', schema)
    assert 'test_catalog.users' in results
    assert len(results['test_catalog.users']) >= 1

def test_policy_propagator_creates_policies():
    classifier = SensitiveDataClassifier()
    classifications = classifier.classify_catalog('catalog', {'table': ['email']})
    propagator = PolicyPropagator()
    propagator.propagate_policies(classifications)
    policies = propagator.get_applied_policies()
    assert 'catalog.table.email' in policies
    assert len(policies['catalog.table.email']) > 0

def test_policy_propagation_downstream():
    classifier = SensitiveDataClassifier()
    classifications = classifier.classify_catalog('catalog', {'source': ['email']})
    propagator = PolicyPropagator()
    lineage = [('catalog.source.email', 'catalog.target.user_email')]
    propagator.propagate_policies(classifications, lineage)
    policies = propagator.get_applied_policies()
    assert 'catalog.target.user_email' in policies
    assert len(policies['catalog.target.user_email']) > 0

def test_policy_severity():
    classifier = SensitiveDataClassifier()
    results = classifier.classify_column('users', 'ssn')
    propagator = PolicyPropagator()
    policy = propagator.create_policy_from_classification(results[0])
    assert policy.severity == 'high'
    assert policy.rules['masking'] == 'required'
