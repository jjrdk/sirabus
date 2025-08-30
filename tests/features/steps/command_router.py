import asyncio
import uuid
from datetime import datetime, timezone

from aett.eventstore import Topic
from behave import step, when, then, use_step_matcher
from steps.command_handlers import StatusCommandHandler, InfoCommandHandler
from steps.test_types import StatusCommand, InvalidCommand, InfoCommand

from features.steps.message_handling import configure_ssl
from sirabus.router.amqp_command_router import (
    AmqpRouterConfiguration,
    AmqpCommandRouter,
)
from sirabus.router.inmemory_command_router import (
    InMemoryRouterConfiguration,
    InMemoryCommandRouter,
)
from sirabus.router.redis_command_router import (
    RedisRouterConfiguration,
    RedisCommandRouter,
)
from sirabus.router.sqs_command_router import SqsRouterConfiguration, SqsCommandRouter

use_step_matcher("re")


@step("commands have been registered in the hierarchical topic map")
def step_impl1(context):
    context.topic_map.add(Topic.get(StatusCommand), StatusCommand)
    context.topic_map.add(Topic.get(InfoCommand), InfoCommand)


@step(
    "a (?P<serializer>.+) (?P<broker_type>.+) router with TLS (?P<use_tls>enabled|disabled) is created with the hierarchical topic map"
)
async def step_impl2(context, serializer, broker_type, use_tls):
    use_tls = use_tls.lower() == "enabled"
    match (serializer, broker_type):
        case ("cloudevent", "amqp"):
            config = configure_ssl(
                AmqpRouterConfiguration.for_cloud_event()
                .with_amqp_url(context.connection_string)
                .with_topic_map(context.topic_map),
                use_tls=use_tls,
            )
            context.router = AmqpCommandRouter(configuration=config)
        case ("pydantic", "amqp"):
            config = configure_ssl(
                AmqpRouterConfiguration.default()
                .with_amqp_url(context.connection_string)
                .with_topic_map(context.topic_map),
                use_tls=use_tls,
            )
            context.router = AmqpCommandRouter(configuration=config)
        case ("cloudevent", "SQS"):
            config = (
                SqsRouterConfiguration.for_cloud_event()
                .with_sqs_config(context.sqs_config)
                .with_topic_map(context.topic_map)
            )

            context.router = SqsCommandRouter(configuration=config)
        case ("pydantic", "SQS"):
            config = (
                SqsRouterConfiguration.default()
                .with_sqs_config(context.sqs_config)
                .with_topic_map(context.topic_map)
            )

            context.router = SqsCommandRouter(configuration=config)
        case ("cloudevent", "in-memory"):
            config = (
                InMemoryRouterConfiguration.for_cloud_event()
                .with_message_pump(context.message_pump)
                .with_topic_map(context.topic_map)
            )

            context.router = InMemoryCommandRouter(configuration=config)
        case ("pydantic", "in-memory"):
            config = (
                InMemoryRouterConfiguration.default()
                .with_message_pump(context.message_pump)
                .with_topic_map(context.topic_map)
            )

            context.router = InMemoryCommandRouter(configuration=config)
        case ("cloudevent", "redis"):
            config = (
                RedisRouterConfiguration.for_cloud_event()
                .with_redis_url(context.connection_string)
                .with_topic_map(context.topic_map)
            )
            context.router = RedisCommandRouter(configuration=config)
        case ("pydantic", "redis"):
            config = (
                RedisRouterConfiguration.default()
                .with_redis_url(context.connection_string)
                .with_topic_map(context.topic_map)
            )
            context.router = RedisCommandRouter(configuration=config)
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
