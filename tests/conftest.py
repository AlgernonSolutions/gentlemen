from datetime import datetime
from decimal import Decimal
from os import path
from unittest import mock
from unittest.mock import patch

import boto3
import pytest
import rapidjson
from boto3.dynamodb.conditions import Attr
from botocore.stub import Stubber

_mock_state_event_id_data = {
    'flow_id': 'some_flow_id',
    'run_id': 'some_run_id',
    'state_reporter': 'some_worker'
}

_mock_state_event_data = {
    'event_id': 'some_flow_id#some_run_id#some_worker',
    'event_timestamp': datetime(2018, 1, 1, 10, 30, 15),
    'event_type': 'FlowStarted',
    'event_details': {}
}


@pytest.fixture
def test_sqs_event():
    return _read_test_event('sqs_receive_message')


@pytest.fixture
def mock_state_event_id_data():
    return _mock_state_event_id_data


@pytest.fixture
def stubbed_dynamo_resource_put_item():
    created_stubs = []
    created_mocks = []

    def build_stubs(test_event):
        dynamo_resource = boto3.resource('dynamodb')
        dynamo_table = dynamo_resource.Table('TestTable')
        dynamo_stub = Stubber(dynamo_table.meta.client)
        put_item_response = _read_test_event('put_dynamo_item_response')
        expected_parameters = {
            'ConditionExpression': Attr('event_id').not_exists() & Attr('event_timestamp').not_exists(),
            'Item': test_event.as_dynamo_item,
            'TableName': 'TEST_TABLE'
        }
        dynamo_stub.add_response('put_item', put_item_response, expected_parameters)
        boto_patch = mock.patch('toll_booth.obj.vinny.sapper.boto3')
        mock_boto = boto_patch.start()
        mock_boto.resource.return_value = dynamo_resource
        dynamo_stub.activate()
        created_stubs.append(dynamo_stub)
        created_mocks.append(boto_patch)
        return mock_boto, dynamo_stub

    yield build_stubs
    for stub in created_stubs:
        stub.deactivate()
    for created_mock in created_mocks:
        created_mock.stop()


@pytest.fixture
def stubbed_sqs_resource_send_message():
    created_stubs = []
    created_mocks = []

    def build_stubs(test_event):
        sqs = boto3.client('sqs')
        sqs_stub = Stubber(sqs)
        send_message_response = _read_test_event('sqs_send_messages_response_all_good')
        # expected_parameters = {
        #    'ConditionExpression': Attr('event_id').not_exists() & Attr('event_timestamp').not_exists(),
        #    'Item': test_event.as_dynamo_item,
        #    'TableName': 'TEST_TABLE'
        # }
        sqs_stub.add_response('send_message_batch', send_message_response)
        boto_patch = mock.patch('toll_booth.obj.continuum.q.boto3')
        mock_boto = boto_patch.start()
        mock_boto.client.return_value = sqs
        sqs_stub.activate()
        created_stubs.append(sqs_stub)
        created_mocks.append(boto_patch)
        return mock_boto, sqs_stub

    yield build_stubs
    for stub in created_stubs:
        stub.deactivate()
    for created_mock in created_mocks:
        created_mock.stop()


@pytest.fixture
def stubbed_sns_resource_publish():
    created_stubs = []
    created_mocks = []

    def build_stubs(test_event):
        dynamo_resource = boto3.resource('dynamodb')
        dynamo_table = dynamo_resource.Table('TestTable')
        dynamo_stub = Stubber(dynamo_table.meta.client)
        put_item_response = _read_test_event('put_dynamo_item_response')
        expected_parameters = {
            'ConditionExpression': Attr('event_id').not_exists() & Attr('event_timestamp').not_exists(),
            'Item': test_event.as_dynamo_item,
            'TableName': 'TEST_TABLE'
        }
        dynamo_stub.add_response('put_item', put_item_response, expected_parameters)
        boto_patch = mock.patch('toll_booth.obj.vinny.sapper.boto3')
        mock_boto = boto_patch.start()
        mock_boto.resource.return_value = dynamo_resource
        dynamo_stub.activate()
        created_stubs.append(dynamo_stub)
        created_mocks.append(boto_patch)
        return mock_boto, dynamo_stub

    yield build_stubs
    for stub in created_stubs:
        stub.deactivate()
    for created_mock in created_mocks:
        created_mock.stop()


@pytest.fixture
def mock_state_event_id():
    from toll_booth.obj.vinny.state_events.events import StateEventId
    return {
        'event_id': StateEventId(**_mock_state_event_id_data),
        'event_timestamp': Decimal(12345234.02),
        'event_type': 'FlowStarted',
        'event_details': {}
    }


@pytest.fixture
def mock_state_event_data():
    from toll_booth.obj.vinny.state_events.events import StateEventId
    event_data = _mock_state_event_data.copy()
    event_data['event_id'] = StateEventId(**_mock_state_event_id_data)
    return event_data


@pytest.fixture
def mock_state_event():
    from toll_booth.obj.vinny.state_events.events import StateEvent
    from toll_booth.obj.vinny.state_events.events import StateEventId
    state_event_id = StateEventId(**_mock_state_event_id_data)
    state_event_data = _mock_state_event_data.copy()
    state_event_data['event_id'] = state_event_id
    event = StateEvent(**state_event_data)
    return event


@pytest.fixture
def api_gw_event():
    return _read_test_event('api_gw_event')


@pytest.fixture
def mock_dynamo_event():
    return _read_test_event('dynamo_update_event')


@pytest.fixture
def mock_sns_event():
    return _read_test_event('sns_notification_event')


@pytest.fixture
def mock_sqs_event():
    return _read_test_event('sqs_receive_message')


@pytest.fixture(autouse=True)
def silence_x_ray():
    x_ray_patch_all = 'algernon.aws.lambda_logging.patch_all'
    patch(x_ray_patch_all).start()
    yield
    patch.stopall()


@pytest.fixture
def mock_context():
    from unittest.mock import MagicMock
    context = MagicMock(name='context')
    context.__reduce__ = cheap_mock
    context.function_name = 'test_function'
    context.invoked_function_arn = 'test_function_arn'
    context.aws_request_id = '12344_request_id'
    context.get_remaining_time_in_millis.side_effect = [1000001, 500001, 250000, 0]
    return context


def cheap_mock(*args):
    from unittest.mock import Mock
    return Mock, ()


def _read_test_event(event_name):
    with open(path.join('tests', 'test_events', f'{event_name}.json')) as json_file:
        event = rapidjson.load(json_file)
        return event
