import os

import boto3
from boto3.dynamodb.conditions import Attr

from toll_booth.obj.vinny.state_events.events import StateEvent


class Sapper:
    def __init__(self, **kwargs):
        self._table_name = kwargs.get('table_name', os.environ['STATE_TABLE_NAME'])
        self._table_resource = boto3.resource('dynamodb').Table(self._table_name)
        self._batch = None

    def __enter__(self):
        with self._table_resource.batch_writer() as batch:
            self._batch = batch
            return batch

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            return True
        raise exc_type(exc_val).with_traceback(exc_tb)

    def add_state_event(self, state_event: StateEvent):
        results = self._table_resource.put_item(
            Item=state_event.as_dynamo_item,
            ConditionExpression=Attr('event_id').not_exists() & Attr('event_timestamp').not_exists()
        )
        return results

    def retrieve_state_history(self, state_event: StateEvent):

        pass
