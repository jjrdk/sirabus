import asyncio
import logging
import uuid
from datetime import datetime, timezone

from aett.eventstore import Topic
from behave import step, when, then, use_step_matcher

from steps.command_handlers import StatusCommandHandler, InfoCommandHandler
from steps.test_types import StatusCommand, InvalidCommand, InfoCommand
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
            StatusCommandHandler(), InfoCommandHandler(),
        ],
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())


@step("commands have been registered in the hierarchical topic map")
def step_impl2(context):
    context.topic_map.add(Topic.get(StatusCommand), StatusCommand)
    context.topic_map.add(Topic.get(InfoCommand), InfoCommand)
    context.router = CloudEventRouter(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
    )


@when('I send the command (?P<topic>.+)')
def step_impl(context, topic):
    command_type = context.topic_map.resolve_type(topic) or InvalidCommand
    context.future = context.async_runner.run_async(
        context.router.route(
            command_type(aggregate_id="test",
                         version=1,
                         timestamp=datetime.now(timezone.utc),
                         correlation_id=str(uuid.uuid4()))))


@then('I should receive the (?P<reply_type>error|reply) "(?P<message>.+?)"')
def step_impl(context, reply_type, message):
    def callback(r):
        context.response = r.result()
        context.wait_handle.set()

    future = context.future
    future.add_done_callback(callback)
    wait_handle: asyncio.Event = context.wait_handle
    assert context.async_runner.run_async(wait_handle.wait()), "Timeout waiting for command response"
    assert context.response.success == (True if reply_type == "reply" else False)
    assert context.response.message == message


@when('I send the commands "(?P<topic1>.+?)", "(?P<topic2>.+?)"')
def step_impl(context, topic1, topic2):
    command_type1 = context.topic_map.resolve_type(topic1)
    context.future1 = context.async_runner.run_async(
        context.router.route(
            command_type1(aggregate_id="test",
                          version=1,
                          timestamp=datetime.now(timezone.utc),
                          correlation_id=str(uuid.uuid4()))))
    command_type2 = context.topic_map.resolve_type(topic2)
    context.future2 = context.async_runner.run_async(
        context.router.route(
            command_type2(aggregate_id="test",
                          version=1,
                          timestamp=datetime.now(timezone.utc),
                          correlation_id=str(uuid.uuid4()))))


@then('I should receive the replies "(?P<msg1>.+?)", "(?P<msg2>.+?)"')
def step_impl(context, msg1, msg2):
    def callback1(r):
        context.response1 = r.result()
        context.wait_handle.set()

    def callback2(r):
        context.response2 = r.result()
        context.wait_handle2.set()

    future1 = context.future1
    future1.add_done_callback(callback1)
    future1 = context.future2
    future1.add_done_callback(callback2)
    assert context.async_runner.run_async(context.wait_handle.wait()), "Timeout waiting for first command response"
    assert context.async_runner.run_async(context.wait_handle2.wait()), "Timeout waiting for second command response"
    assert context.response1.message == msg1
    assert context.response2.message == msg2
