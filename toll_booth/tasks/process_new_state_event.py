import logging


from toll_booth.obj.vinny.sapper import Sapper
from algernon.aws import lambda_logged
from algernon import ajson, queued
from algernon.aws import Bullhorn

_decision_triggers = (
    'WorkflowStarted',
    'DecisionScheduled',
    'ScheduleTaskFailed',
    'StartTaskFailed',
    'TaskFailed',
    'TaskCompleted'
)
_task_triggers = (
    'TaskScheduled'
)


@lambda_logged
@queued
def task(event, context):
    logging.info(f'started a process_new_state_event call, event: {event}')
    _add_state_event(event)
    state_history = _retrieve_state_history(event)
    event_type = event.event_type
    processing_action = 'decision'
    if event_type in _task_triggers:
        processing_action = 'task'
    if event_type in _decision_triggers or event_type in _task_triggers:
        _send_event_for_processing(processing_action, event, state_history)
    logging.info(f'completed a process_new_state_event call, event: {event}')


def _add_state_event(state_event):
    logging.info(f'going to add state_event: {state_event} to the state table')
    sapper = Sapper()
    sapper.add_state_event(state_event)
    logging.info(f'added the state_event: {state_event} to the state table')


def _retrieve_state_history(state_event):
    logging.info(f'going to retrieve the state history for event: {state_event} from the state table')
    sapper = Sapper()
    state_history = sapper.retrieve_state_history(state_event)
    logging.info(f'retrieved the state history: {state_history} for event: {state_event} from the state table')
    return state_history


def _send_event_for_processing(processing_action, state_event, state_history):
    logging.info(f'going to send state_event: {state_event} '
                 f'with state history: {state_history} for processing and execution')
    bullhorn = Bullhorn()
    processing_endpoint_arn = state_history.processing_endpoint_arn
    message = {'state_event': state_event, 'state_history': state_history}
    encoded_message = ajson.dumps(message)
    bullhorn.publish(processing_action, processing_endpoint_arn, encoded_message)
    logging.info(f'sent message: {message} to endpoint_arn: {processing_endpoint_arn}')
