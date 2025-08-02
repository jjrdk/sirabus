import logging
import uuid
from datetime import datetime, timezone

from aett.eventstore import Topic
from behave import step, when, then, use_step_matcher

from features.steps.command_handlers import StatusCommandHandler
from features.steps.test_types import StatusCommand
from sirabus import CommandResponse
from sirabus.publisher.cloudevent_router import CloudEventRouter
from sirabus.servicebus.cloudevent_servicebus import create_servicebus_for_amqp_cloudevent
from sirabus.topography import TopographyBuilder

use_step_matcher("re")


@step("amqp router is configured with the hierarchical topic map")
def step_impl3(context):
    builder = TopographyBuilder(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    context.async_runner.run_async(builder.build())
    bus = create_servicebus_for_amqp_cloudevent(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
        event_handlers=[
            StatusCommandHandler()
        ],
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())
    logging.debug("Topography built.")


@step("commands have been registered in the hierarchical topic map")
def step_impl2(context):
    context.topic_map.add(Topic.get(StatusCommand), StatusCommand)


@when('I send the command (?P<message>.+)')
def step_impl(context, message):
    router = CloudEventRouter(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
    )
    response: CommandResponse = context.async_runner.run_async(
        router.route(
            StatusCommand(aggregate_id="test",
                          version=1,
                          timestamp=datetime.now(timezone.utc),
                          correlation_id=str(uuid.uuid4()))))
    assert response.success
    assert response.message == message


@then('I should receive the response (?P<response>.+)')
def step_impl(context, response):
    pass
