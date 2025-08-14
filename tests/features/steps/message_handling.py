import datetime
import logging
import uuid
import asyncio
from behave import given, when, then, step, use_step_matcher
from testcontainers.localstack import LocalStackContainer
from testcontainers.rabbitmq import RabbitMqContainer

from tests.features.steps.command_handlers import StatusCommandHandler
from sirabus import generate_vhost_name
from sirabus.servicebus.cloudevent_servicebus import (
    create_servicebus_for_amqp_cloudevent,
    create_servicebus_for_memory_cloudevent,
)
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


@given("a running amqp message broker")
def step_impl1(context):
    logging.basicConfig(level=logging.DEBUG)
    context.wait_handle = asyncio.Event()
    context.wait_handle2 = asyncio.Event()
    context.topic_map = HierarchicalTopicMap()
    container = RabbitMqContainer(vhost=generate_vhost_name("test", "0.0.0")).start()
    context.containers.append(container)
    params = container.get_connection_params()
    creds = params.credentials
    virtual_host = (
        "%2F" if params.virtual_host == "/" else params.virtual_host.strip("/")
    )
    context.connection_string = f"amqp://{creds.username}:{creds.password}@{params.host}:{params.port}/{virtual_host}"


@given("a running SQS message broker")
def step_impl1(context):
    logging.basicConfig(level=logging.DEBUG)
    context.wait_handle = asyncio.Event()
    context.wait_handle2 = asyncio.Event()
    context.topic_map = HierarchicalTopicMap()
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


@given("a running in-memory message broker")
def step_impl2(context):
    logging.basicConfig(level=logging.DEBUG)
    context.wait_handle = asyncio.Event()
    context.topic_map = HierarchicalTopicMap()
    context.handlers = [
        TestEventHandler(wait_handle=context.wait_handle),
        StatusCommandHandler(),
    ]


@step("events have been registered in the hierarchical topic map")
def step_impl3(context):
    context.topic_map.register(TestEvent)
    context.topic_map.register(SubTestEvent)
    context.topic_map.register(OtherTestEvent)
    context.topic_map.register(NestedTestEvent)


@step("a cloudevent amqp broker is configured with the hierarchical topic map")
def step_impl4(context):
    builder = AmqpTopographyBuilder(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    context.async_runner.run_async(builder.build())
    bus = create_servicebus_for_amqp_cloudevent(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
        handlers=[
            TestEventHandler(wait_handle=context.wait_handle),
            OtherTestEventHandler(wait_handle=context.wait_handle2),
        ],
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())
    logging.debug("Topography built.")


@step("a cloudevent SQS broker is configured with the hierarchical topic map")
def step_impl5(context):
    builder = SqsTopographyBuilder(context.topic_map, context.sqs_config)
    builder.build()
    from sirabus.servicebus.sqs_servicebus import create_servicebus_for_sqs_cloudevent

    bus = create_servicebus_for_sqs_cloudevent(
        config=context.sqs_config,
        topic_map=context.topic_map,
        handlers=[
            TestEventHandler(wait_handle=context.wait_handle),
            OtherTestEventHandler(wait_handle=context.wait_handle2),
        ],
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())
    logging.debug("Topography built.")


@step("a pydantic amqp broker is configured with the hierarchical topic map")
def step_impl6(context):
    builder = AmqpTopographyBuilder(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    context.async_runner.run_async(builder.build())
    from sirabus.servicebus import create_servicebus_for_amqp_pydantic

    bus = create_servicebus_for_amqp_pydantic(
        amqp_url=context.connection_string,
        topic_map=context.topic_map,
        event_handlers=[
            TestEventHandler(wait_handle=context.wait_handle),
            OtherTestEventHandler(wait_handle=context.wait_handle2),
        ],
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())
    logging.debug("Topography built.")


@step("a cloudevent in-memory broker is configured with the hierarchical topic map")
def step_impl7(context):
    context.messagepump = MessagePump()
    context.messagepump.start()
    bus = create_servicebus_for_memory_cloudevent(
        topic_map=context.topic_map,
        handlers=context.handlers,
        message_pump=context.messagepump,
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())


@step("a pydantic in-memory broker is configured with the hierarchical topic map")
def step_impl8(context):
    context.messagepump = MessagePump()
    context.messagepump.start()
    from sirabus.servicebus.pydantic_servicebus import create_servicebus_for_inmemory

    bus = create_servicebus_for_inmemory(
        topic_map=context.topic_map,
        handlers=context.handlers,
        message_pump=context.messagepump,
    )
    context.consumer = bus
    context.async_runner.run_async(bus.run())


@when("I send a cloudevent (?P<topic>.+) message to the amqp service bus")
def step_impl9(context, topic):
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    from sirabus.publisher.cloudevent_publisher import (
        create_publisher_for_amqp,
    )

    publisher = create_publisher_for_amqp(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    context.async_runner.run_async(publisher.publish(event))


@when("I send a cloudevent (?P<topic>.+) message to the SQS service bus")
def step_impl10(context, topic):
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    from sirabus.publisher.sqs_publisher import create_publisher_for_sqs

    publisher = create_publisher_for_sqs(
        config=context.sqs_config, topic_map=context.topic_map
    )
    context.async_runner.run_async(publisher.publish(event))


@when("I send a pydantic (?P<topic>.+) message to the amqp service bus")
def step_impl11(context, topic):
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    from sirabus.publisher.pydantic_publisher import create_publisher_for_amqp

    publisher = create_publisher_for_amqp(
        amqp_url=context.connection_string, topic_map=context.topic_map
    )
    context.async_runner.run_async(publisher.publish(event))


@when("I send a cloudevent (?P<topic>.+) message to the in-memory service bus")
def step_impl12(context, topic):
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    from sirabus.publisher.cloudevent_publisher import (
        create_publisher_for_inmemory,
    )

    publisher = create_publisher_for_inmemory(
        topic_map=context.topic_map, message_pump=context.messagepump
    )
    context.async_runner.run_async(publisher.publish(event))


@when("I send a pydantic (?P<topic>.+) message to the in-memory service bus")
def step_impl13(context, topic):
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    from sirabus.publisher.pydantic_publisher import (
        create_publisher_for_inmemory,
    )

    publisher = create_publisher_for_inmemory(
        topic_map=context.topic_map, message_pump=context.messagepump
    )
    context.async_runner.run_async(publisher.publish(event))


@then("the message is received by the subscriber")
def step_impl14(context):
    try:
        context.async_runner.run_async(asyncio.sleep(0.25))
        assert context.async_runner.run_async( context.wait_handle.wait()), "The message was not received by the subscriber in time"
    finally:
        context.async_runner.run_async(context.consumer.stop())


@step("the other event handlers are not invoked")
def step_impl15(context):
    assert context.wait_handle2.is_set() is False, (
        "The other event handler was invoked, but it should not have been"
    )


@then("the messages are received by the subscriber")
def step_impl16(context):
    assert context.wait_handle2.is_set() and context.wait_handle.is_set(), (
        "The message was not received by the subscriber in time"
    )
