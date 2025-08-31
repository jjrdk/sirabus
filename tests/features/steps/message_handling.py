import asyncio
import datetime
import logging
import pathlib
import uuid

from behave import given, when, then, step, use_step_matcher
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.localstack import LocalStackContainer
from testcontainers.rabbitmq import RabbitMqContainer
from testcontainers.redis import RedisContainer

from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.publisher.amqp_publisher import AmqpPublisherConfiguration
from sirabus.topography.amqp import TopographyBuilder as AmqpTopographyBuilder
from sirabus.topography.sqs import TopographyBuilder as SqsTopographyBuilder, SqsConfig
from tests.features.steps.command_handlers import (
    StatusCommandHandler,
    InfoCommandHandler,
)
from tests.features.steps.test_types import (
    TestEvent,
    TestEventHandler,
    SubTestEvent,
    OtherTestEvent,
    NestedTestEvent,
    OtherTestEventHandler,
)

use_step_matcher("re")


def generate_vhost_name(name: str, version: str) -> str:
    """
    Generates a virtual host name based on the application name and version.
    :param name: The name of the application.
    :param version: The version of the application.
    :return: A string representing the virtual host name.
    """
    import hashlib

    h = hashlib.sha256(usedforsecurity=False)
    h.update(f"{name}_{version}".encode())
    return h.hexdigest()


@given(
    "a running (?P<broker_type>.+) message broker with TLS (?P<use_tls>enabled|disabled)"
)
def step_impl1(context, broker_type, use_tls):
    use_tls = use_tls == "enabled"
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
            set_up_amqp_broker(context=context, use_tls=use_tls)
        case "SQS":
            set_up_sqs_broker(context=context, use_tls=use_tls)
        case "in-memory":
            pass
        case "redis":
            set_up_redis(context=context, use_tls=use_tls)
        case _:
            raise ValueError(f"Unknown broker type: {broker_type}")


def set_up_amqp_broker(context, use_tls: bool = False):
    """Sets up a RabbitMQ container for testing."""
    container = RabbitMqContainer(vhost=generate_vhost_name("test", "0.0.0"))
    if use_tls:
        current_dir = pathlib.Path(__file__).resolve().parent
        container = (
            container.with_exposed_ports(5671, 5672)
            .with_volume_mapping(
                f"{current_dir}/../../configs/rabbitmq.conf",
                "/etc/rabbitmq/rabbitmq.conf",
            )
            .with_volume_mapping(
                f"{current_dir}/../../configs/certs", "/etc/rabbitmq/certs"
            )
        )
    container = container.start()

    context.containers.append(container)
    params = container.get_connection_params()

    creds = params.credentials
    virtual_host = (
        "%2F" if params.virtual_host == "/" else params.virtual_host.strip("/")
    )
    context.connection_string = f"{'amqps' if use_tls else 'amqp'}://{creds.username}:{creds.password}@{params.host}:{container.get_exposed_port(5671) if use_tls else params.port}/{virtual_host}"


def set_up_sqs_broker(context, use_tls: bool = False):
    container = (
        LocalStackContainer(image="localstack/localstack:latest")
        .with_env("DEBUG", "1")
        .with_services("sns", "sqs")
        .with_volume_mapping(
            f"{pathlib.Path(__file__).resolve().parent}/../../configs/certs",
            "/var/lib/localstack/cache",
        )
        .start()
    )
    context.containers.append(container)
    context.sqs_config = SqsConfig(
        container.env["AWS_ACCESS_KEY_ID"],
        container.env["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=None,
        profile_name=None,
        region=container.region_name,
        endpoint_url=container.get_url().replace("http://", "https://")
        if use_tls
        else container.get_url(),
        alternate_ca_bundle=f"{pathlib.Path(__file__).resolve().parent}/../../configs/certs/ca_certificate.pem"
        if use_tls
        else None,
    )


def set_up_redis(context, use_tls: bool = False):
    if use_tls:
        current_dir = pathlib.Path(__file__).resolve().parent
        image = DockerImage(
            path=f"{current_dir}/../../configs",
            dockerfile_path="redis_tls.Dockerfile",
            tag="redis_tls:latest",
        )
        image.build()
        container = (
            DockerContainer(image="redis_tls:latest")
            .with_volume_mapping(f"{current_dir}/../../configs/certs", "/tls")
            .with_exposed_ports(6379)
            .start()
        )
    else:
        container = RedisContainer().start()
    context.connection_string = f"{'rediss' if use_tls else 'redis'}://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}"
    context.containers.append(container)


@step("events have been registered in the hierarchical topic map")
def step_impl2(context):
    context.topic_map.register(TestEvent)
    context.topic_map.register(SubTestEvent)
    context.topic_map.register(OtherTestEvent)
    context.topic_map.register(NestedTestEvent)


@step(
    "a (?P<serializer>.+?) (?P<broker_type>.+) service bus with TLS (?P<use_tls>enabled|disabled) is configured with the hierarchical topic map"
)
async def step_impl3(context, serializer, broker_type, use_tls):
    use_tls = use_tls == "enabled"
    match (serializer, broker_type):
        case ("cloudevent", "amqp"):
            await configure_cloudevent_amqp_service_bus(
                context=context, use_tls=use_tls
            )
        case ("pydantic", "amqp"):
            await configure_pydantic_amqp_service_bus(context=context, use_tls=use_tls)
        case ("cloudevent", "SQS"):
            configure_cloudevent_sqs_service_bus(context=context, use_tls=use_tls)
        case ("pydantic", "SQS"):
            configure_pydantic_sqs_service_bus(context=context, use_tls=use_tls)
        case ("cloudevent", "in-memory"):
            configure_cloudevent_inmemory_service_bus(context=context, use_tls=use_tls)
        case ("pydantic", "in-memory"):
            configure_pydantic_inmemory_service_bus(context=context, use_tls=use_tls)
        case ("cloudevent", "redis"):
            configure_cloudevent_redis_service_bus(context=context, use_tls=use_tls)
        case ("pydantic", "redis"):
            configure_pydantic_redis_service_bus(context=context, use_tls=use_tls)

    await context.consumer.run()
    logging.debug("Topography built.")


def configure_ssl(config, use_tls: bool):
    if not use_tls:
        return config
    import ssl

    certs_path = (
        pathlib.Path(__file__).parent
        / ".."
        / ".."
        / "configs"
        / "certs"
        / "ca_certificate.pem"
    )
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=certs_path
    )
    ssl_context.check_hostname = False
    return config.with_ssl_config(ssl_context).with_ca_cert_file(str(certs_path))


async def configure_cloudevent_amqp_service_bus(context, use_tls: bool = False):
    configuration = configure_ssl(
        AmqpPublisherConfiguration.default()
        .with_amqp_url(context.connection_string)
        .with_topic_map(context.topic_map),
        use_tls,
    )
    builder = AmqpTopographyBuilder(configuration=configuration)
    await builder.build()

    from sirabus.servicebus.amqp_servicebus import (
        AmqpServiceBusConfiguration,
        AmqpServiceBus,
    )

    config = configure_ssl(
        AmqpServiceBusConfiguration.for_cloud_event()
        .with_topic_map(context.topic_map)
        .with_amqp_url(context.connection_string)
        .with_handlers(*context.handlers),
        use_tls=use_tls,
    )
    context.consumer = AmqpServiceBus(config)


async def configure_pydantic_amqp_service_bus(context, use_tls: bool = False):
    configuration = configure_ssl(
        AmqpPublisherConfiguration.default()
        .with_amqp_url(context.connection_string)
        .with_topic_map(context.topic_map),
        use_tls,
    )
    builder = AmqpTopographyBuilder(configuration=configuration)
    await builder.build()

    from sirabus.servicebus.amqp_servicebus import (
        AmqpServiceBusConfiguration,
        AmqpServiceBus,
    )

    config = configure_ssl(
        AmqpServiceBusConfiguration.default()
        .with_amqp_url(context.connection_string)
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers),
        use_tls=use_tls,
    )
    context.consumer = AmqpServiceBus(config)


def configure_cloudevent_sqs_service_bus(context, use_tls: bool = False):
    builder = SqsTopographyBuilder(context.topic_map, context.sqs_config)
    builder.build()

    from sirabus.servicebus.sqs_servicebus import (
        SqsServiceBusConfiguration,
        SqsServiceBus,
    )

    config = configure_ssl(
        SqsServiceBusConfiguration.for_cloud_event()
        .with_sqs_config(context.sqs_config)
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers),
        use_tls=use_tls,
    )
    context.consumer = SqsServiceBus(config)


def configure_pydantic_sqs_service_bus(context, use_tls: bool = False):
    builder = SqsTopographyBuilder(context.topic_map, context.sqs_config)
    builder.build()

    from sirabus.servicebus.sqs_servicebus import (
        SqsServiceBusConfiguration,
        SqsServiceBus,
    )

    config = configure_ssl(
        SqsServiceBusConfiguration.default()
        .with_sqs_config(context.sqs_config)
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers),
        use_tls=use_tls,
    )
    context.consumer = SqsServiceBus(config)


def configure_cloudevent_inmemory_service_bus(context, use_tls: bool = False):
    context.message_pump = MessagePump()
    context.message_pump.start()

    from sirabus.servicebus.inmemory_servicebus import (
        InMemoryConfiguration,
        InMemoryServiceBus,
    )

    config = configure_ssl(
        InMemoryConfiguration.for_cloud_event()
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers)
        .with_message_pump(context.message_pump),
        use_tls=use_tls,
    )
    context.consumer = InMemoryServiceBus(config)


def configure_cloudevent_redis_service_bus(context, use_tls: bool = False):
    from sirabus.servicebus.redis_servicebus import (
        RedisServiceBusConfiguration,
        RedisServiceBus,
    )

    config = configure_ssl(
        RedisServiceBusConfiguration.for_cloud_event()
        .with_redis_url(context.connection_string)
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers),
        use_tls=use_tls,
    )
    context.consumer = RedisServiceBus(config)


def configure_pydantic_redis_service_bus(context, use_tls: bool = False):
    from sirabus.servicebus.redis_servicebus import (
        RedisServiceBusConfiguration,
        RedisServiceBus,
    )

    config = configure_ssl(
        RedisServiceBusConfiguration.default()
        .with_redis_url(context.connection_string)
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers),
        use_tls=use_tls,
    )
    context.consumer = RedisServiceBus(config)


def configure_pydantic_inmemory_service_bus(context, use_tls: bool = False):
    context.message_pump = MessagePump()
    context.message_pump.start()

    from sirabus.servicebus.inmemory_servicebus import (
        InMemoryConfiguration,
        InMemoryServiceBus,
    )

    config = configure_ssl(
        InMemoryConfiguration.default()
        .with_topic_map(context.topic_map)
        .with_handlers(*context.handlers)
        .with_message_pump(context.message_pump),
        use_tls=use_tls,
    )
    context.consumer = InMemoryServiceBus(config)


@when(
    "I send a (?P<serializer>.+) (?P<topic>.+) message to the (?P<broker_type>.+) service bus with TLS (?P<use_tls>enabled|disabled)"
)
async def step_impl4(context, serializer, topic, broker_type, use_tls):
    use_tls = use_tls == "enabled"
    event_type = context.topic_map.get(topic)
    event = event_type(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        correlation_id=str(uuid.uuid4()),
    )
    publisher = None
    match (serializer, broker_type):
        case ("cloudevent", "amqp"):
            publisher = create_cloudevent_amqp_publisher(
                context=context, use_tls=use_tls
            )
        case ("pydantic", "amqp"):
            publisher = create_pydantic_amqp_publisher(context=context, use_tls=use_tls)
        case ("cloudevent", "SQS"):
            publisher = create_cloudevent_sqs_publisher(
                context=context, use_tls=use_tls
            )
        case ("pydantic", "SQS"):
            publisher = create_pydantic_sqs_publisher(context=context, use_tls=use_tls)
        case ("cloudevent", "in-memory"):
            publisher = create_cloudevent_inmemory_publisher(context)
        case ("pydantic", "in-memory"):
            publisher = create_pydantic_inmemory_publisher(context)
        case ("cloudevent", "redis"):
            publisher = create_cloudevent_redis_publisher(
                context=context, use_tls=use_tls
            )
        case ("pydantic", "redis"):
            publisher = create_pydantic_redis_publisher(
                context=context, use_tls=use_tls
            )
    await publisher.publish(event)


def create_cloudevent_amqp_publisher(context, use_tls: bool = False):
    from sirabus.publisher.amqp_publisher import (
        AmqpPublisherConfiguration,
        AmqpPublisher,
    )

    config = configure_ssl(
        AmqpPublisherConfiguration.for_cloud_event()
        .with_amqp_url(context.connection_string)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return AmqpPublisher(config)


def create_pydantic_amqp_publisher(context, use_tls: bool = False):
    from sirabus.publisher.amqp_publisher import (
        AmqpPublisherConfiguration,
        AmqpPublisher,
    )

    config = configure_ssl(
        AmqpPublisherConfiguration.default()
        .with_amqp_url(context.connection_string)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return AmqpPublisher(config)


def create_cloudevent_sqs_publisher(context, use_tls: bool = False):
    from sirabus.publisher.sqs_publisher import SqsPublisherConfiguration, SqsPublisher

    config = configure_ssl(
        SqsPublisherConfiguration.for_cloud_event()
        .with_sqs_config(context.sqs_config)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return SqsPublisher(config)


def create_pydantic_sqs_publisher(context, use_tls: bool = False):
    from sirabus.publisher.sqs_publisher import SqsPublisherConfiguration, SqsPublisher

    config = configure_ssl(
        SqsPublisherConfiguration.default()
        .with_sqs_config(context.sqs_config)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return SqsPublisher(config)


def create_cloudevent_inmemory_publisher(context, use_tls: bool = False):
    from sirabus.publisher.inmemory_publisher import (
        InMemoryPublisherConfiguration,
        InMemoryPublisher,
    )

    config = configure_ssl(
        InMemoryPublisherConfiguration.for_cloud_event()
        .with_message_pump(context.message_pump)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return InMemoryPublisher(config)


def create_cloudevent_redis_publisher(context, use_tls: bool = False):
    from sirabus.publisher.redis_publisher import (
        RedisPublisher,
        RedisPublisherConfiguration,
    )

    config = configure_ssl(
        RedisPublisherConfiguration.for_cloud_event()
        .with_redis_url(context.connection_string)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return RedisPublisher(config)


def create_pydantic_inmemory_publisher(context):
    from sirabus.publisher.inmemory_publisher import (
        InMemoryPublisherConfiguration,
        InMemoryPublisher,
    )

    config = (
        InMemoryPublisherConfiguration.default()
        .with_message_pump(context.message_pump)
        .with_topic_map(context.topic_map)
    )
    return InMemoryPublisher(config)


def create_pydantic_redis_publisher(context, use_tls: bool = False):
    from sirabus.publisher.redis_publisher import (
        RedisPublisher,
        RedisPublisherConfiguration,
    )

    config = configure_ssl(
        RedisPublisherConfiguration.default()
        .with_redis_url(context.connection_string)
        .with_topic_map(context.topic_map),
        use_tls=use_tls,
    )
    return RedisPublisher(config)


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
