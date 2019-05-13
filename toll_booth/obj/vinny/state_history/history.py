class StateHistory:
    def __init__(self, flow_type, flow_id, flow_run_id, task_args, history, events, markers):
        self._flow_type = flow_type
        self._flow_id = flow_id
        self._flow_run_id = flow_run_id
        self._task_args = task_args
        self._history = history
        self._events = events
        self._markers = markers

    @classmethod
    def parse_from_dynamo(cls, dynamo_records):
        pass

    @property
    def flow_type(self):
        return self._flow_type

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def flow_run_id(self):
        return self._flow_run_id

    @property
    def task_args(self):
        return self._task_args

    @property
    def history(self):
        return self._history

    @property
    def events(self):
        return self._events

    @property
    def markers(self):
        return self._markers
