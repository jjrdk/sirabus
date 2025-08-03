import logging
from typing import List

from sirabus import IHandleEvents, IHandleCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.servicebus import ServiceBus
from sirabus.servicebus.inmemory_servicebus import InMemoryServiceBus


def create_servicebus_for_amqp(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    prefetch_count: int = 10,
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
