import logging
from typing import List, Optional

from sirabus import IHandleEvents, IHandleCommands, SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.servicebus import ServiceBus
from sirabus.servicebus.inmemory_servicebus import InMemoryServiceBus


def create_servicebus_for_amqp(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    prefetch_count: int = 10,
    logger: Optional[logging.Logger] = None,
) -> ServiceBus:
    """
    Create a ServiceBus instance for AMQP.
    :param amqp_url: The AMQP URL for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param prefetch_count: The number of messages to prefetch from the service bus.
    :param logger: Optional logger for logging.
    :return: An instance of AmqpServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    """
    from sirabus.servicebus.amqp_servicebus import AmqpServiceBus

    from sirabus.publisher.pydantic_serialization import (
        create_command_response,
        read_event_message,
    )

    return AmqpServiceBus(
        amqp_url=amqp_url,
        topic_map=topic_map,
        handlers=handlers,
        prefetch_count=prefetch_count,
        message_reader=read_event_message,
        command_response_writer=create_command_response,
        logger=logger or logging.getLogger("AmqpServiceBus"),
    )


def create_servicebus_for_sqs(
    config: SqsConfig,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    prefetch_count: int = 10,
    logger: Optional[logging.Logger] = None,
) -> ServiceBus:
    """
    Create a ServiceBus instance for SQS.
    :param config: The SQS configuration for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param prefetch_count: The number of messages to prefetch from the service bus.
    :param logger: Optional logger for logging.
    :return: An instance of SqsServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    """
    from sirabus.publisher.pydantic_serialization import (
        create_command_response,
        read_event_message,
    )

    from sirabus.servicebus.sqs_servicebus import SqsServiceBus

    return SqsServiceBus(
        config=config,
        topic_map=topic_map,
        handlers=handlers,
        message_reader=read_event_message,
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
    Create a ServiceBus instance for SQS.
    :param redis_url: The Redis URL for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param logger: Optional logger for logging.
    :return: An instance of SqsServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    """
    from sirabus.publisher.pydantic_serialization import (
        create_command_response,
        read_event_message,
    )

    from sirabus.servicebus.redis_servicebus import RedisServiceBus

    return RedisServiceBus(
        redis_url=redis_url,
        topic_map=topic_map,
        handlers=handlers,
        message_reader=read_event_message,
        command_response_writer=create_command_response,
        logger=logger or logging.getLogger("RedisServiceBus"),
    )


def create_servicebus_for_inmemory(
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    message_pump: MessagePump,
) -> ServiceBus:
    """
    Create a ServiceBus instance for in-memory communication.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param handlers: A list of event and command handlers.
    :param message_pump: The message pump for handling message consumption and publishing.
    :return: An instance of InMemoryServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    """
    from sirabus.publisher.pydantic_serialization import (
        create_command_response,
        read_event_message,
    )

    return InMemoryServiceBus(
        topic_map=topic_map,
        handlers=handlers,
        message_reader=read_event_message,
        response_writer=create_command_response,
        message_pump=message_pump,
        logger=logging.getLogger("InMemoryServiceBus"),
    )
