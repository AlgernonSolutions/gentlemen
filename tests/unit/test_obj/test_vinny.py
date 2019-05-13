from datetime import datetime

from toll_booth.obj.vinny.state_events.events import StateEventId, StateEvent


class TestStateEventId:
    def test_state_event_id(self, mock_state_event_id_data):
        flow_id = mock_state_event_id_data['flow_id']
        run_id = mock_state_event_id_data['run_id']
        state_reporter = mock_state_event_id_data['state_reporter']
        state_event_id = StateEventId(flow_id, run_id, state_reporter)
        expected_string = f'{flow_id}#{run_id}#{state_reporter}'
        assert isinstance(state_event_id, StateEventId)
        assert str(state_event_id) == expected_string
        assert state_event_id.flow_id == mock_state_event_id_data['flow_id']
        assert state_event_id.run_id == mock_state_event_id_data['run_id']
        assert state_event_id.state_reporter == mock_state_event_id_data['state_reporter']
        assert state_event_id.flow_run_id == f'{mock_state_event_id_data["flow_id"]}#{mock_state_event_id_data["run_id"]}'

    def test_state_event_id_from_raw(self, mock_state_event_id_data):
        flow_id = mock_state_event_id_data['flow_id']
        run_id = mock_state_event_id_data['run_id']
        state_reporter = mock_state_event_id_data['state_reporter']
        state_event_id = StateEventId(flow_id, run_id, state_reporter)
        string_event_id = str(state_event_id)
        regenerated_event_id = StateEventId.parse_from_raw(string_event_id)
        assert str(regenerated_event_id) == str(string_event_id)
        regenerated_from_id = StateEventId.parse_from_raw(state_event_id)
        assert str(regenerated_from_id) == str(state_event_id)


class TestStateEvent:
    def test_state_event(self, mock_state_event_data):
        state_event = StateEvent(**mock_state_event_data)
        assert isinstance(state_event, StateEvent)
        assert state_event.event_id == mock_state_event_data['event_id']
        assert state_event.event_timestamp == mock_state_event_data['event_timestamp']
        assert state_event.event_type == mock_state_event_data['event_type']
        assert state_event.event_details == mock_state_event_data['event_details']
        assert state_event.as_dynamo_item

    def test_state_event_internals(self, mock_state_event):
        assert isinstance(getattr(mock_state_event, '_event_timestamp'), datetime)
