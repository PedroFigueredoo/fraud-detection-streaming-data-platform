import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from event_contract import load_contract, serialize_event, valid_json, validation_errors
from producer_account_applications import create_event, replay


SCHEMA = load_contract(Path('/opt/airflow/replay/schemas/account_application_v1.json'))
ROW = dict(month='0', customer_age='40', income='0.3', employment_status='CB', fraud_bool='0')


def test_event_serialization():
    event = create_event(ROW, 1, 'test')
    assert json.loads(serialize_event(event, SCHEMA)) == event
    assert event['customer_age'] == 40
    assert event['event_id'] == 'test:1'


@pytest.mark.parametrize('field,value', [('fraud_bool', 2), ('month', 8), ('income', float('nan')), ('income', 2), ('schema_version', 2), ('customer_age', '40')])
def test_reject_invalid_values(field, value):
    event = create_event(ROW, 1, 'test')
    event[field] = value
    with pytest.raises(ValueError):
        serialize_event(event, SCHEMA)
    assert not valid_json(json.dumps(event), SCHEMA)


def test_missing_and_malformed():
    event = create_event(ROW, 1, 'test')
    del event['month']
    assert not valid_json(json.dumps(event), SCHEMA)
    assert not valid_json('{broken', SCHEMA)


@pytest.mark.parametrize('field,value,reason', [
    ('customer_age', 'bad', 'Invalid type: customer_age'),
    ('fraud_bool', 2, 'Invalid enum: fraud_bool'),
    ('month', 8, 'Out of range: month'),
    ('schema_version', 2, 'Invalid enum: schema_version'),
])
def test_rejection_reason(field, value, reason):
    event = create_event(ROW, 1, 'reason')
    event[field] = value
    assert validation_errors(json.dumps(event), SCHEMA) == [reason]


def test_missing_malformed_and_null_reasons():
    event = create_event(ROW, 1, 'reason')
    del event['month']
    assert validation_errors(json.dumps(event), SCHEMA) == ['Missing fields: month']
    assert validation_errors('{broken', SCHEMA) == ['Malformed JSON']
    assert validation_errors(None, SCHEMA) == ['Null payload']


def test_bounded_replay_and_publication_failure(tmp_path):
    source = tmp_path / 'sample.csv'
    source.write_text('month,customer_age,income,employment_status,fraud_bool\n0,40,0.3,CB,0\ninvalid\n')
    producer = Mock()
    assert replay(source, 1, 0, 'unit', SCHEMA, producer, 'topic') == 1
    assert producer.send.call_count == 1
    producer.send.return_value.get.side_effect = TimeoutError('broker unavailable')
    with pytest.raises(TimeoutError):
        replay(source, 1, 0, 'unit', SCHEMA, producer, 'topic')
