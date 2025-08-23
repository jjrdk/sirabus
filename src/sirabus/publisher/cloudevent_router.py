import logging
from typing import Optional

from sirabus import IRouteCommands, SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.publisher.cloudevent_serialization import read_command_response


def create_amqp_router(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    logger: Optional[logging.Logger] = None,
) -> IRouteCommands:
    """
    Creates a CloudEventCommandRouter for AMQP.
    :param amqp_url: The AMQP URL.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: An AmqpCommandRouter instance.
    :raises ValueError: If the amqp_url is empty or if the topic_map is None.
    :raises TypeError: If message_writer or response_reader are not callable.
    """
    from sirabus.publisher.cloudevent_serialization import create_command
    from sirabus.publisher.amqp_command_router import AmqpCommandRouter

    return AmqpCommandRouter(
        amqp_url=amqp_url,
        topic_map=topic_map,
        logger=logger,
        message_writer=create_command,
        response_reader=read_command_response,
    )


def create_sqs_router(
    config: SqsConfig,
    topic_map: HierarchicalTopicMap,
    logger: Optional[logging.Logger] = None,
) -> IRouteCommands:
    """
    Creates a CloudEventCommandRouter for SQS.
    :param config: The SQS configuration.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: An SqsCommandRouter instance.
    :raises ValueError: If the config is None or if the topic_map is None.
    :raises TypeError: If message_writer or response_reader are not callable.
    """
    from sirabus.publisher.cloudevent_serialization import create_command
    from sirabus.publisher.sqs_command_router import SqsCommandRouter

    return SqsCommandRouter(
        config=config,
        topic_map=topic_map,
        logger=logger,
        message_writer=create_command,
        response_reader=read_command_response,
    )


def create_redis_router(
    redis_url: str,
    topic_map: HierarchicalTopicMap,
    logger: Optional[logging.Logger] = None,
) -> IRouteCommands:
    """
    Creates a CloudEventCommandRouter for Redis.
    :param redis_url: The Redis URL for Pub/Sub.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: An RedisCommandRouter instance.
    :raises ValueError: If the config is None or if the topic_map is None.
    :raises TypeError: If message_writer or response_reader are not callable.
    """
    from sirabus.publisher.cloudevent_serialization import create_command
    from sirabus.publisher.redis_command_router import RedisCommandRouter

    return RedisCommandRouter(
        redis_url=redis_url,
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
    """
    Creates a CloudEventCommandRouter for in-memory use.
    :param message_pump: The message pump for handling messages.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: An InMemoryCommandRouter instance.
    :raises ValueError: If the message_pump is None or if the topic_map is None.
    :raises TypeError: If message_writer or response_reader are not callable.
    """
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
