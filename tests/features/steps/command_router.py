import asyncio
import uuid
from datetime import datetime, timezone

from aett.eventstore import Topic
from behave import step, when, then, use_step_matcher
from steps.command_handlers import StatusCommandHandler, InfoCommandHandler
from steps.test_types import StatusCommand, InvalidCommand, InfoCommand

use_step_matcher("re")


@step("commands have been registered in the hierarchical topic map")
def step_impl1(context):
    context.topic_map.add(Topic.get(StatusCommand), StatusCommand)
    context.topic_map.add(Topic.get(InfoCommand), InfoCommand)


@step(
    "a (?P<serializer>.+) (?P<broker_type>.+) router is created with the hierarchical topic map"
)
async def step_impl2(context, serializer, broker_type):
    match (serializer, broker_type):
        case ("cloudevent", "amqp"):
            from sirabus.publisher.cloudevent_router import create_amqp_router

            context.router = create_amqp_router(
                amqp_url=context.connection_string,
                topic_map=context.topic_map,
            )
        case ("pydantic", "amqp"):
            from sirabus.publisher.pydantic_router import create_amqp_router

            context.router = create_amqp_router(
                amqp_url=context.connection_string,
                topic_map=context.topic_map,
            )
        case ("cloudevent", "SQS"):
            from sirabus.publisher.cloudevent_router import create_sqs_router

            context.router = create_sqs_router(
                config=context.sqs_config,
                topic_map=context.topic_map,
            )
        case ("pydantic", "SQS"):
            from sirabus.publisher.pydantic_router import create_sqs_router

            context.router = create_sqs_router(
                config=context.sqs_config,
                topic_map=context.topic_map,
            )
        case ("cloudevent", "in-memory"):
            from sirabus.publisher.cloudevent_router import create_inmemory_router

            context.router = create_inmemory_router(
                message_pump=context.message_pump,
                topic_map=context.topic_map,
            )
        case ("pydantic", "in-memory"):
            from sirabus.publisher.pydantic_router import create_inmemory_router

            context.router = create_inmemory_router(
                message_pump=context.message_pump,
                topic_map=context.topic_map,
            )
        case _:
            raise ValueError(f"Unknown broker type: {serializer} {broker_type}")
    await asyncio.sleep(0.1)


@when("I send the command (?P<topic>.+)")
async def step_impl3(context, topic):
    command_type = context.topic_map.get(topic) or InvalidCommand
    context.future = await context.router.route(
        command_type(
            aggregate_id="test",
            version=1,
            timestamp=datetime.now(timezone.utc),
            correlation_id=str(uuid.uuid4()),
        )
    )


@then('I receive the (?P<reply_type>error|reply) "(?P<message>.+?)"')
async def step_impl4(context, reply_type, message):
    def callback(r):
        context.response = r.result()
        context.wait_handle.set()

    future = context.future
    future.add_done_callback(callback)
    for i in range(10):
        if context.wait_handle.is_set():
            break
        # Allow some time for the command to be processed
        await asyncio.sleep(0.25)
    assert context.wait_handle.is_set(), "Timeout waiting for command response"
    assert context.response.success == (True if reply_type == "reply" else False)
    assert context.response.message == message


@when('I send the commands "(?P<topic1>.+?)", "(?P<topic2>.+?)"')
async def step_impl5(context, topic1, topic2):
    command_type1 = context.topic_map.get(topic1)
    context.future1 = await context.router.route(
        command_type1(
            aggregate_id="test",
            version=1,
            timestamp=datetime.now(timezone.utc),
            correlation_id=str(uuid.uuid4()),
        )
    )
    command_type2 = context.topic_map.get(topic2)
    context.future2 = await context.router.route(
        command_type2(
            aggregate_id="test",
            version=1,
            timestamp=datetime.now(timezone.utc),
            correlation_id=str(uuid.uuid4()),
        )
    )


@then('I receive the replies "(?P<msg1>.+?)", "(?P<msg2>.+?)"')
async def step_impl6(context, msg1, msg2):
    def callback1(r):
        context.response1 = r.result()
        context.wait_handle.set()

    def callback2(r):
        context.response2 = r.result()
        context.wait_handle2.set()

    future1 = context.future1
    future1.add_done_callback(callback1)
    future2 = context.future2
    future2.add_done_callback(callback2)
    for i in range(10):
        if context.wait_handle.is_set() and context.wait_handle2.is_set():
            break
        # Allow some time for the command to be processed
        await asyncio.sleep(0.25)
    assert context.wait_handle.is_set(), "Timeout waiting for first command response"
    assert context.wait_handle2.is_set(), "Timeout waiting for second command response"
    assert context.response1.message == msg1, (
        f"Expected {msg1}, got {context.response1.message}"
    )
    assert context.response2.message == msg2, (
        f"Expected {msg2}, got {context.response2.message}"
    )
