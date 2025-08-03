import logging
from typing import Optional

from sirabus import IRouteCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.publisher.amqp_command_router import AmqpCommandRouter
from sirabus.publisher.cloudevent_serialization import read_command_response


def create_amqp_router(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    logger: Optional[logging.Logger] = None,
) -> IRouteCommands:
    from sirabus.publisher.cloudevent_serialization import create_command

    return AmqpCommandRouter(
        amqp_url=amqp_url,
        topic_map=topic_map,
        logger=logger,
        message_writer=create_command,
        response_reader=read_command_response,
    )


def create_inmemory_router(
    message_pump: MessagePump,
    topic_map: HierarchicalTopicMap,
    logger: Optional[logging.Logger] = None,
) -> IRouteCommands:
    from sirabus.publisher.cloudevent_serialization import (
        create_command,
        read_command_response,
    )
    from sirabus.publisher.inmemory_command_router import InMemoryCommandRouter

    return InMemoryCommandRouter(
        message_pump=message_pump,
        topic_map=topic_map,
        logger=logger,
        command_writer=create_command,
        response_reader=read_command_response,
    )
