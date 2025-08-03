import datetime
import logging
import uuid
import asyncio
from aett.eventstore import Topic
from behave import given, when, then, step, use_step_matcher
from testcontainers.rabbitmq import RabbitMqContainer

from tests.features.steps.command_handlers import StatusCommandHandler
from sirabus import generate_vhost_name
from sirabus.servicebus.cloudevent_servicebus import (
    create_servicebus_for_amqp_cloudevent,
    create_servicebus_for_memory_cloudevent,
)
from sirabus.message_pump import MessagePump
from sirabus.topography import TopographyBuilder
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
    container = RabbitMqContainer(vhost=generate_vhost_name("test", "0.0.0"))
    container.start()
    context.containers.append(container)
    params = container.get_connection_params()
    creds = params.credentials
    virtual_host = (
        "%2F" if params.virtual_host == "/" else params.virtual_host.strip("/")
    )
    context.connection_string = f"amqp://{creds.username}:{creds.password}@{params.host}:{params.port}/{virtual_host}"


@given("a running in-memory message broker")
def step_impl(context):
    logging.basicConfig(level=logging.DEBUG)
    context.wait_handle = asyncio.Event()
    context.topic_map = HierarchicalTopicMap()
    context.handlers = [
        TestEventHandler(wait_handle=context.wait_handle),
        StatusCommandHandler(),
    ]


@step("events have been registered in the hierarchical topic map")
def step_impl2(context):
    context.topic_map.add(Topic.get(TestEvent), TestEvent)
    context.topic_map.add(Topic.get(SubTestEvent), SubTestEvent)
    context.topic_map.add(Topic.get(OtherTestEvent), OtherTestEvent)
    context.topic_map.add(Topic.get(NestedTestEvent), NestedTestEvent)


@step("a cloudevent amqp broker is configured with the hierarchical topic map")
def step_impl3(context):
    builder = TopographyBuilder(
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


@step("a pydantic amqp broker is configured with the hierarchical topic map")
def step_impl10(context):
    builder = TopographyBuilder(
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
def step_impl4(context):
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
def step_impl4(context):
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
def step_impl5(context, topic):
    event_type = context.topic_map.resolve_type(topic)
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


@when("I send a pydantic (?P<topic>.+) message to the amqp service bus")
def step_impl5(context, topic):
    event_type = context.topic_map.resolve_type(topic)
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
def step_impl6(context, topic):
    event_type = context.topic_map.resolve_type(topic)
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
def step_impl6(context, topic):
    event_type = context.topic_map.resolve_type(topic)
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
def step_impl7(context):
    try:
        context.async_runner.run_async(asyncio.sleep(0.25))
        result = context.async_runner.run_async(context.wait_handle.wait())
        assert result, "The message was not received by the subscriber in time"
    finally:
        context.async_runner.run_async(context.consumer.stop())


@step("the other event handlers are not invoked")
def step_impl8(context):
    assert context.wait_handle2.is_set() is False, (
        "The other event handler was invoked, but it should not have been"
    )


@then("the messages are received by the subscriber")
def step_impl9(context):
    assert context.wait_handle2.is_set() and context.wait_handle.is_set(), (
        "The message was not received by the subscriber in time"
    )
