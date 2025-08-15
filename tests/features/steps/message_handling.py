import datetime
import logging
import uuid
import asyncio
from behave import given, when, then, step, use_step_matcher
from testcontainers.localstack import LocalStackContainer
from testcontainers.rabbitmq import RabbitMqContainer

from tests.features.steps.command_handlers import (
    StatusCommandHandler,
    InfoCommandHandler,
)
from sirabus import generate_vhost_name
from sirabus.message_pump import MessagePump
from sirabus.topography.amqp import TopographyBuilder as AmqpTopographyBuilder
from sirabus.topography.sqs import TopographyBuilder as SqsTopographyBuilder, SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from tests.features.steps.test_types import (
    TestEvent,
    TestEventHandler,
    SubTestEvent,
    OtherTestEvent,
    NestedTestEvent,
    OtherTestEventHandler,
)

use_step_matcher("re")


@given("a running (?P<broker_type>.+) message broker")
def step_impl1(context, broker_type):
    logging.basicConfig(level=logging.DEBUG)
    context.wait_handle = asyncio.Event()
    context.wait_handle2 = asyncio.Event()
    context.topic_map = HierarchicalTopicMap()
    context.handlers = [
        TestEventHandler(wait_handle=context.wait_handle),
        OtherTestEventHandler(wait_handle=context.wait_handle2),
        StatusCommandHandler(),
        InfoCommandHandler(),
    ]
    match broker_type:
        case "amqp":
            set_up_amqp_broker(context)
        case "SQS":
            set_up_sqs_broker(context)
        case "in-memory":
            pass
        case _:
            raise ValueError(f"Unknown broker type: {broker_type}")


def set_up_amqp_broker(context):
    """Sets up a RabbitMQ container for testing."""
    container = RabbitMqContainer(vhost=generate_vhost_name("test", "0.0.0")).start()
    context.containers.append(container)
    params = container.get_connection_params()
    creds = params.credentials
    virtual_host = (
        "%2F" if params.virtual_host == "/" else params.virtual_host.strip("/")
    )
    context.connection_string = f"amqp://{creds.username}:{creds.password}@{params.host}:{params.port}/{virtual_host}"


def set_up_sqs_broker(context):
    container = (
        LocalStackContainer(image="localstack/localstack:latest")
        .with_services("sns", "sqs")
        .start()
    )
    context.containers.append(container)
    context.sns_client = container.get_client("sns")
    context.sqs_client = container.get_client("sqs")
    context.sqs_config = SqsConfig(
        container.env["AWS_ACCESS_KEY_ID"],
        container.env["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=None,
        profile_name=None,
        region=container.region_name,
        endpoint_url=context.sqs_client.meta.endpoint_url,
        use_tls=False,
    )


@step("events have been registered in the hierarchical topic map")
def step_impl2(context):
    context.topic_map.register(TestEvent)
    context.topic_map.register(SubTestEvent)
    context.topic_map.register(OtherTestEvent)
    context.topic_map.register(NestedTestEvent)


@step(
    "a (?P<serializer>.+?) (?P<broker_type>.+) service bus is configured with the hierarchical topic map"
)
async def step_impl3(context, serializer, broker_type):
    match (serializer, broker_type):
        case ("cloudevent", "amqp"):
            await configure_cloudevent_amqp_service_bus(context)
        case ("pydantic", "amqp"):
            await configure_pydantic_amqp_service_bus(context)
        case ("cloudevent", "SQS"):
            configure_cloudevent_sqs_service_bus(context)
        case ("pydantic", "SQS"):
            configure_pydantic_sqs_service_bus(context)
        case ("cloudevent", "in-memory"):
            configure_cloudevent_inmemory_service_bus(context)
        case ("pydantic", "in-memory"):
            configure_pydantic_inmemory_service_bus(context)

    await context.consumer.run()
    logging.debug("Topography built.")


async def configure_cloudevent_amqp_service_bus(context):
    builder = AmqpTopographyBuilder(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    await builder.build()
    from sirabus.servicebus.cloudevent_servicebus import (
        create_servicebus_for_amqp_cloudevent,
    )

    bus = create_servicebus_for_amqp_cloudevent(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
        handlers=context.handlers,
    )
    context.consumer = bus


async def configure_pydantic_amqp_service_bus(context):
    builder = AmqpTopographyBuilder(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    await builder.build()
    from sirabus.servicebus import create_servicebus_for_amqp_pydantic

    bus = create_servicebus_for_amqp_pydantic(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
        event_handlers=context.handlers,
    )
    context.consumer = bus


def configure_cloudevent_sqs_service_bus(context):
    builder = SqsTopographyBuilder(context.topic_map, context.sqs_config)
    builder.build()
    from sirabus.servicebus.cloudevent_servicebus import create_servicebus_for_sqs

    bus = create_servicebus_for_sqs(
        config=context.sqs_config,
        topic_map=context.topic_map,
        handlers=context.handlers,
    )
    context.consumer = bus


def configure_pydantic_sqs_service_bus(context):
    builder = SqsTopographyBuilder(context.topic_map, context.sqs_config)
    builder.build()

    from sirabus.servicebus.pydantic_servicebus import create_servicebus_for_sqs

    bus = create_servicebus_for_sqs(
        config=context.sqs_config,
        topic_map=context.topic_map,
        handlers=context.handlers,
    )
    context.consumer = bus


def configure_cloudevent_inmemory_service_bus(context):
    context.message_pump = MessagePump()
    context.message_pump.start()
    from sirabus.servicebus.cloudevent_servicebus import create_servicebus_for_inmemory

    bus = create_servicebus_for_inmemory(
        topic_map=context.topic_map,
        handlers=context.handlers,
        message_pump=context.message_pump,
    )
    context.consumer = bus


def configure_pydantic_inmemory_service_bus(context):
    context.message_pump = MessagePump()
    context.message_pump.start()
    from sirabus.servicebus.pydantic_servicebus import create_servicebus_for_inmemory

    bus = create_servicebus_for_inmemory(
        topic_map=context.topic_map,
        handlers=context.handlers,
        message_pump=context.message_pump,
    )
    context.consumer = bus


@when(
    "I send a (?P<serializer>.+) (?P<topic>.+) message to the (?P<broker_type>.+) service bus"
)
async def step_impl4(context, serializer, topic, broker_type):
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    publisher = None
    match (serializer, broker_type):
        case ("cloudevent", "amqp"):
            publisher = create_cloudevent_amqp_publisher(context)
        case ("pydantic", "amqp"):
            publisher = create_pydantic_amqp_publisher(context)
        case ("cloudevent", "SQS"):
            publisher = create_cloudevent_sqs_publisher(context)
        case ("pydantic", "SQS"):
            publisher = create_pydantic_sqs_publisher(context)
        case ("cloudevent", "in-memory"):
            publisher = create_cloudevent_inmemory_publisher(context)
        case ("pydantic", "in-memory"):
            publisher = create_pydantic_inmemory_publisher(context)
    await publisher.publish(event)


def create_cloudevent_amqp_publisher(context):
    from sirabus.publisher.cloudevent_publisher import create_publisher_for_amqp

    return create_publisher_for_amqp(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )


def create_pydantic_amqp_publisher(context):
    from sirabus.publisher.pydantic_publisher import create_publisher_for_amqp

    return create_publisher_for_amqp(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )


def create_cloudevent_sqs_publisher(context):
    from sirabus.publisher.cloudevent_publisher import create_publisher_for_sqs

    return create_publisher_for_sqs(
        config=context.sqs_config, topic_map=context.topic_map
    )


def create_pydantic_sqs_publisher(context):
    from sirabus.publisher.pydantic_publisher import create_publisher_for_sqs

    return create_publisher_for_sqs(
        config=context.sqs_config, topic_map=context.topic_map
    )


def create_cloudevent_inmemory_publisher(context):
    from sirabus.publisher.cloudevent_publisher import create_publisher_for_inmemory

    return create_publisher_for_inmemory(
        message_pump=context.message_pump, topic_map=context.topic_map
    )


def create_pydantic_inmemory_publisher(context):
    from sirabus.publisher.pydantic_publisher import create_publisher_for_inmemory

    return create_publisher_for_inmemory(
        message_pump=context.message_pump, topic_map=context.topic_map
    )


@then("the message is received by the subscriber")
async def step_impl5(context):
    result = False
    for i in range(10):
        result = context.wait_handle.is_set()
        if result:
            break
        await asyncio.sleep(0.25)
    assert result, "The message was not received by the subscriber in time"
    await context.consumer.stop()


@step("the other event handlers are not invoked")
def step_impl6(context):
    assert context.wait_handle2.is_set() is False, (
        "The other event handler was invoked, but it should not have been"
    )


@then("the messages are received by the subscriber")
async def step_impl7(context):
    result = False
    for i in range(10):
        result = context.wait_handle2.is_set() and context.wait_handle.is_set()
        if result:
            break
        await asyncio.sleep(0.25)
    assert result, "The message was not received by the subscriber in time"
    await context.consumer.stop()
