import importlib
from decimal import Decimal

from algernon import AlgObject


class StateEvent(AlgObject):
    def __init__(self, event_id, event_timestamp, event_type, event_details):
        self._event_id = event_id
        self._event_timestamp = event_timestamp
        self._event_type = event_type
        self._event_details = event_details

    @classmethod
    def parse_from_dynamo(cls, dynamo_record):
        event_type = dynamo_record['event_type']
        host_module = importlib.import_module(cls.__module__)
        event_class = getattr(host_module, event_type)
        event_id = StateEventId.parse_from_raw(dynamo_record['event_id'])
        class_kwargs = {
            'event_timestamp': dynamo_record['event_timestamp']
        }
        class_kwargs.update(dynamo_record['event_details'])
        return event_class(*event_id.id_values, **class_kwargs)

    @classmethod
    def parse_json(cls, json_dict):
        event_details = json_dict['event_details']
        event_id = json_dict['event_id']
        del(json_dict['event_details'])
        del(json_dict['event_id'])
        json_dict.update(event_details)
        return cls(*event_id.id_values, **json_dict)

    @property
    def flow_id(self):
        return self._event_id.flow_id

    @property
    def flow_run_id(self):
        return self._event_id.flow_run_id

    @property
    def event_id(self):
        return self._event_id

    @property
    def event_timestamp(self):
        return self._event_timestamp

    @property
    def event_type(self):
        return self._event_type

    @property
    def event_details(self):
        return self._event_details

    @property
    def source_event_id(self):
        if self._event_type == 'WorkflowStarted':
            return None
        return self._event_details['source_event_id']

    @property
    def as_dynamo_item(self):
        try:
            event_timestamp = Decimal(self._event_timestamp.timestamp())
        except AttributeError:
            event_timestamp = Decimal(self._event_timestamp)
        return {
            'event_id': str(self._event_id),
            'event_timestamp': event_timestamp,
            'event_type': self._event_type,
            'event_details': self._event_details
        }


class WorkflowStarted(StateEvent):
    def __init__(self, flow_id, flow_run_id, event_timestamp, **kwargs):
        event_type = 'WorkflowStarted'
        event_details = {
            'flow_type': kwargs['flow_type'],
            'input_args': kwargs['input_args']
        }
        event_id = StateEventId(event_type, flow_id, flow_run_id)
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def flow_type(self):
        return self._event_details['flow_type']

    @property
    def input_args(self):
        return self._event_details['input_args']


class WorkflowCompleted(StateEvent):
    def __init__(self, flow_id, flow_run_id, event_timestamp, **kwargs):
        event_type = 'WorkflowCompleted'
        event_id = StateEventId(event_type, flow_id, flow_run_id)
        event_details = {
            'flow_results': kwargs['flow_results'],
            'source_event_id': kwargs['source_event_id']
        }
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def flow_results(self):
        return self._event_details['flow_results']


class DecisionScheduled(StateEvent):
    def __init__(self, flow_id, flow_run_id, event_timestamp, **kwargs):
        event_type = 'DecisionScheduled'
        event_id = StateEventId(event_type, flow_id, flow_run_id)
        event_details = {
            'source_event_id': kwargs['source_event_id']
        }
        super().__init__(event_id, event_timestamp, event_type, event_details)


class DecisionStarted(StateEvent):
    def __init__(self, event_timestamp, flow_id, flow_run_id, decision_scheduled_id):
        event_type = 'DecisionStarted'
        event_id = StateEventId(event_type, flow_id, flow_run_id)
        event_details = {
            'source_event_id': decision_scheduled_id
        }
        super().__init__(event_id, event_timestamp, event_type, event_details)


class DecisionCompleted(StateEvent):
    def __init__(self, event_timestamp, flow_id, flow_run_id, decision_started_id, decisions):
        event_type = 'DecisionCompleted'
        event_id = StateEventId(event_type, flow_id, flow_run_id)
        event_details = {
            'decisions': decisions,
            'source_event_id': decision_started_id
        }
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def decisions(self):
        return self._event_details['decisions']


class TaskScheduled(StateEvent):
    def __init__(self, flow_id, flow_run_id, task_id, event_timestamp, **kwargs):
        event_type = 'TaskScheduled'
        event_id = StateEventId(event_type, flow_id, flow_run_id, task_id)
        event_details = {
            'source_event_id': kwargs['source_event_id'],
            'task_type': kwargs['task_type'],
            'task_list': kwargs['task_list'],
            'input_args': kwargs['input_args']
        }
        self._task_id = task_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def task_id(self):
        return self._task_id

    @property
    def task_type(self):
        return self._event_details['task_type']

    @property
    def task_list(self):
        return self._event_details['task_list']

    @property
    def input_args(self):
        return self._event_details['input_args']


class ScheduleTaskFailed(StateEvent):
    def __init__(self, flow_id, flow_run_id, task_id, event_timestamp, **kwargs):
        event_type = 'ScheduleTaskFailed'
        event_id = StateEventId(event_type, flow_id, flow_run_id, task_id)
        event_details = {
            'source_event_id': kwargs['source_event_id'],
            'fail_category': kwargs['fail_category'],
            'fail_reason': kwargs['fail_reason']
        }
        self._task_id = task_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def task_id(self):
        return self._task_id

    @property
    def fail_category(self):
        return self._event_details['fail_category']

    @property
    def fail_reason(self):
        return self._event_details['fail_reason']


class TaskStarted(StateEvent):
    def __init__(self, flow_id, flow_run_id, task_id, task_run_id, event_timestamp, **kwargs):
        event_type = 'TaskStarted'
        event_id = StateEventId(event_type, flow_id, flow_run_id, task_id, task_run_id)
        event_details = {
            'source_event_id': kwargs['source_event_id'],
            'task_type': kwargs['task_type'],
            'task_listener_arn': kwargs['task_listener_arn'],
            'input_args': kwargs['input_args']
        }
        self._task_id = task_id
        self._task_run_id = task_run_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def task_id(self):
        return self._task_id

    @property
    def task_run_id(self):
        return self._task_run_id

    @property
    def task_type(self):
        return self._event_details['event_type']

    @property
    def task_listener_arn(self):
        return self._event_details['task_listener_arn']

    @property
    def input_args(self):
        return self._event_details['input_args']


class StartTaskFailed(StateEvent):
    def __init__(self, flow_id, flow_run_id, task_id, task_run_id, event_timestamp, **kwargs):
        event_type = 'StartTaskFailed'
        event_id = StateEventId(event_type, flow_id, flow_run_id, task_id, task_run_id)
        event_details = {
            'source_event_id': kwargs['source_event_id'],
            'fail_category': kwargs['fail_category'],
            'fail_reason': kwargs['fail_reason']
        }
        self._task_run_id = task_run_id
        self._task_id = task_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def task_id(self):
        return self._task_id

    @property
    def task_run_id(self):
        return self._task_run_id

    @property
    def fail_category(self):
        return self._event_details['fail_category']

    @property
    def fail_reason(self):
        return self._event_details['fail_reason']


class TaskFailed(StateEvent):
    def __init__(self, flow_id, flow_run_id, task_id, task_run_id, event_timestamp, **kwargs):
        event_type = 'TaskFailed'
        event_id = StateEventId(event_type, flow_id, flow_run_id, task_id, task_run_id)
        event_details = {
            'source_event_id': kwargs['source_event_id'],
            'fail_category': kwargs['fail_category'],
            'fail_reason': kwargs['fail_reason']
        }
        self._task_run_id = task_run_id
        self._task_id = task_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def task_id(self):
        return self._task_id

    @property
    def task_run_id(self):
        return self._task_run_id

    @property
    def fail_category(self):
        return self._event_details['fail_category']

    @property
    def fail_reason(self):
        return self._event_details['fail_reason']


class TaskCompleted(StateEvent):
    def __init__(self, flow_id, flow_run_id, task_id, task_run_id, event_timestamp, **kwargs):
        event_type = 'TaskCompleted'
        event_id = StateEventId(event_type, flow_id, flow_run_id, task_id, task_run_id)
        event_details = {
            'source_event_id': kwargs['source_event_id'],
            'task_results': kwargs['task_results']
        }
        self._task_run_id = task_run_id
        self._task_id = task_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def task_id(self):
        return self._task_id

    @property
    def task_run_id(self):
        return self._task_run_id

    @property
    def task_results(self):
        return self._event_details('task_results')


class MarkerRecorded(StateEvent):
    def __init__(self, flow_id, flow_run_id, marker_id, event_timestamp, marker_type, marker_details):
        event_type = 'MarkerRecorded'
        event_id = StateEventId(event_type, flow_id, flow_run_id, marker_id)
        event_details = {
            'marker_type': marker_type,
            'marker_details': marker_details
        }
        self._marker_id = marker_id
        super().__init__(event_id, event_timestamp, event_type, event_details)

    @property
    def marker_id(self):
        return self._marker_id


class StateEventId(AlgObject):
    def __init__(self, event_type, flow_id, flow_run_id, *args):
        self._flow_id = flow_id
        self._flow_run_id = flow_run_id
        id_values = [flow_id, flow_run_id]
        id_values.extend(args)
        self._id_values = id_values
        self._event_type = event_type

    @classmethod
    def parse_from_raw(cls, raw_event_id):
        if isinstance(raw_event_id, cls):
            return raw_event_id
        if '!' in raw_event_id:
            pieces = raw_event_id.split('!')
            id_values = pieces[0].split('#')
            return cls(pieces[1], *id_values)
        return cls(*raw_event_id.split('#'))

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['event_type'], *json_dict['id_values'])

    @property
    def event_type(self):
        return self._event_type

    @property
    def id_values(self):
        return self._id_values

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def flow_run_id(self):
        return self._flow_run_id

    def __str__(self):
        return f'{"#".join([str(x) for x in self._id_values])}!{self._event_type}'
