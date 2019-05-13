import pytest

from toll_booth.tasks import process_new_state_event


@pytest.mark.process_new_state_event
class TestProcessNewStateEvent:
    def test_process_new_state_event(self, test_sqs_event, mock_context):
        process_new_state_event.task(test_sqs_event, mock_context)
