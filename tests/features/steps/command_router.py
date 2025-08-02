import asyncio
import logging
import uuid
from datetime import datetime, timezone

from aett.eventstore import Topic
from behave import step, when, then, use_step_matcher

from features.steps.command_handlers import StatusCommandHandler
from features.steps.test_types import StatusCommand
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
    context.router = CloudEventRouter(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
    )
    context.future = context.async_runner.run_async(
        context.router.route(
            StatusCommand(aggregate_id="test",
                          version=1,
                          timestamp=datetime.now(timezone.utc),
                          correlation_id=str(uuid.uuid4()))))


@then('I should receive the response (?P<message>.+)')
def step_impl(context, message):
    def callback(r):
        context.response = r.result()
        context.wait_handle.set()

    future = context.future
    future.add_done_callback(callback)
    wait_handle: asyncio.Event = context.wait_handle
    assert context.async_runner.run_async(wait_handle.wait()), "Timeout waiting for command response"
    assert context.response.success
    assert context.response.message == message
