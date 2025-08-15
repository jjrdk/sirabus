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


def create_servicebus_for_inmemory(
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    message_pump: MessagePump,
) -> ServiceBus:
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
