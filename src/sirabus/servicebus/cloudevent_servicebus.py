import logging
from typing import List, Optional

from sirabus import IHandleEvents, IHandleCommands, SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.servicebus import ServiceBus
from sirabus.servicebus.inmemory_servicebus import InMemoryServiceBus


def create_servicebus_for_amqp_cloudevent(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    prefetch_count: int = 10,
) -> ServiceBus:
    """
    Create a ServiceBus instance for AMQP using CloudEvents serialization.
    :param amqp_url: The AMQP URL for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param prefetch_count: The number of messages to prefetch from the service bus.
    :return: An instance of AmqpServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    :note: This function uses CloudEvents serialization for message handling.
    :example:
        >>> from sirabus import create_servicebus_for_amqp_cloudevent, HierarchicalTopicMap, IHandleEvents
        >>> import logging
        >>> class MyEventHandler(IHandleEvents):
        ...     async def handle(self, event, headers):
        ...         print(f"Handling event: {event} with headers: {headers}")
        >>> topic_map = HierarchicalTopicMap()
        >>> handlers = [MyEventHandler()]
        >>> amqp_url = "amqp://guest:guest@localhost/"
        >>> service_bus = create_servicebus_for_amqp_cloudevent(
        ...     amqp_url=amqp_url,
        ...     topic_map=topic_map,
        ...     handlers=handlers,
        ...     prefetch_count=10,
        ... )
        >>> logging.basicConfig(level=logging.DEBUG)
        >>> service_bus.run()
    :note: The `run` method starts the service bus and begins consuming messages from RabbitMQ.
           The `stop` method should be called to gracefully shut down the service bus and close
           the connection to RabbitMQ.
    """
    from sirabus.servicebus.amqp_servicebus import AmqpServiceBus
    from sirabus.publisher.cloudevent_serialization import (
        write_cloudevent_message,
        create_command_response,
    )

    return AmqpServiceBus(
        amqp_url=amqp_url,
        topic_map=topic_map,
        handlers=handlers,
        prefetch_count=prefetch_count,
        message_reader=write_cloudevent_message,
        command_response_writer=create_command_response,
    )


def create_servicebus_for_sqs(
    config: SqsConfig,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    prefetch_count: int = 10,
    logger: Optional[logging.Logger] = None,
) -> ServiceBus:
    """
    Create a ServiceBus instance for SQS using CloudEvents serialization.
    :param config: The SQS configuration for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param prefetch_count: The number of messages to prefetch from the service bus.
    :param logger: Optional logger for logging.
    :return: An instance of SqsServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    :note: This function uses CloudEvents serialization for message handling.
    :example:
        >>> from sirabus import create_servicebus_for_sqs, SqsConfig, HierarchicalTopicMap, IHandleEvents
        >>> import logging
        >>> class MyEventHandler(IHandleEvents):
        ...     async def handle(self, event, headers):
        ...         print(f"Handling event: {event} with headers: {headers}")
        >>> config = SqsConfig(
        ...     aws_access_key_id="your_access_key",
        ...     aws_secret_access_key="your_secret_key",
        ...     region="us-east-1",
        ... )
        >>> topic_map = HierarchicalTopicMap()
        >>> handlers = [MyEventHandler()]
        >>> service_bus = create_servicebus_for_sqs(
        ...     config=config,
        ...     topic_map=topic_map,
        ...     handlers=handlers,
        ...     prefetch_count=10,
        ...     logger=logging.getLogger("SqsServiceBus"),
        ... )
        >>> logging.basicConfig(level=logging.DEBUG)
        >>> service_bus.run()
    :note: The `run` method starts the service bus and begins consuming messages from SQS.
           The `stop` method should be called to gracefully shut down the service bus and close
           the connection to SQS.
    """
    from sirabus.publisher.cloudevent_serialization import (
        write_cloudevent_message,
        create_command_response,
    )

    from sirabus.servicebus.sqs_servicebus import SqsServiceBus

    return SqsServiceBus(
        config=config,
        topic_map=topic_map,
        handlers=handlers,
        message_reader=write_cloudevent_message,
        command_response_writer=create_command_response,
        prefetch_count=prefetch_count,
        logger=logger or logging.getLogger("SqsServiceBus"),
    )


def create_servicebus_for_redis(
    redis_url: str,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    logger: Optional[logging.Logger] = None,
) -> ServiceBus:
    """
    Create a ServiceBus instance for Redis using CloudEvents serialization.
    :param redis_url: The Redis URL for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param logger: Optional logger for logging.
    :return: An instance of RedisServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    :note: This function uses CloudEvents serialization for message handling.
    :note: The `run` method starts the service bus and begins consuming messages from Redis.
           The `stop` method should be called to gracefully shut down the service bus and close
           the connection to Redis.
    """
    from sirabus.publisher.cloudevent_serialization import (
        write_cloudevent_message,
        create_command_response,
    )

    from sirabus.servicebus.redis_servicebus import RedisServiceBus

    return RedisServiceBus(
        redis_url=redis_url,
        topic_map=topic_map,
        handlers=handlers,
        message_reader=write_cloudevent_message,
        command_response_writer=create_command_response,
        logger=logger or logging.getLogger("RedisServiceBus"),
    )


def create_servicebus_for_inmemory(
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    message_pump: MessagePump,
) -> ServiceBus:
    """
    Create a ServiceBus instance for in-memory message handling using CloudEvents serialization.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param message_pump: The message pump for processing messages.
    :return: An instance of InMemoryServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    :note: This function uses CloudEvents serialization for message handling.
    :example:
        >>> from sirabus import create_servicebus_for_inmemory, HierarchicalTopicMap, IHandleEvents
        >>> import logging
        >>> class MyEventHandler(IHandleEvents):
        ...     async def handle(self, event, headers):
        ...         print(f"Handling event: {event} with headers: {headers}")
        >>> topic_map = HierarchicalTopicMap()
        >>> handlers = [MyEventHandler()]
        >>> message_pump = MessagePump()
        >>> service_bus = create_servicebus_for_inmemory(
        ...     topic_map=topic_map,
        ...     handlers=handlers,
        ...     message_pump=message_pump,
        ... )
        >>> logging.basicConfig(level=logging.DEBUG)
        >>> service_bus.run()
    :note: The `run` method starts the service bus and begins processing messages in memory.
           The `stop` method should be called to gracefully shut down the service bus.
    """
    from sirabus.publisher.cloudevent_serialization import create_command_response

    from sirabus.publisher.cloudevent_serialization import write_cloudevent_message

    return InMemoryServiceBus(
        topic_map=topic_map,
        handlers=handlers,
        message_reader=write_cloudevent_message,
        response_writer=create_command_response,
        message_pump=message_pump,
        logger=logging.getLogger("InMemoryServiceBus"),
    )
