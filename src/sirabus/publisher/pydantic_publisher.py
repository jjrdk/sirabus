import logging

from sirabus import IPublishEvents, SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.publisher.inmemory_publisher import InMemoryPublisher


def create_publisher_for_amqp(
    amqp_url: str, topic_map: HierarchicalTopicMap, logger: logging.Logger | None = None
) -> IPublishEvents:
    """
    Creates a CloudEventPublisher for AMQP.
    :param amqp_url: The AMQP URL.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    from sirabus.publisher.pydantic_serialization import create_event
    from sirabus.publisher.amqp_publisher import AmqpPublisher

    return AmqpPublisher(
        amqp_url=amqp_url, topic_map=topic_map, logger=logger, event_writer=create_event
    )


def create_publisher_for_sqs(
    config: SqsConfig,
    topic_map: HierarchicalTopicMap,
    logger: logging.Logger | None = None,
) -> IPublishEvents:
    """
    Creates a CloudEventPublisher for SQS.
    :param config: The SQS configuration.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    from sirabus.publisher.pydantic_serialization import create_event
    from sirabus.publisher.sqs_publisher import SqsPublisher

    return SqsPublisher(
        sqs_config=config, topic_map=topic_map, logger=logger, event_writer=create_event
    )


def create_publisher_for_inmemory(
    message_pump: MessagePump,
    topic_map: HierarchicalTopicMap,
    logger: logging.Logger | None = None,
) -> IPublishEvents:
    """
    Creates a CloudEventPublisher for AMQP.
    :param message_pump: The message pump to use for in-memory publishing.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    from sirabus.publisher.pydantic_serialization import create_event

    return InMemoryPublisher(
        messagepump=message_pump,
        topic_map=topic_map,
        logger=logger,
        event_writer=create_event,
    )


def create_publisher_for_redis(
    redis_url: str,
    topic_map: HierarchicalTopicMap,
    logger: logging.Logger | None = None,
) -> IPublishEvents:
    """
    Creates a CloudEventPublisher for SQS.
    :param redis_url: The Redis connection URL.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    from sirabus.publisher.pydantic_serialization import create_event
    from sirabus.publisher.redis_publisher import RedisPublisher

    return RedisPublisher(
        redis_url=redis_url,
        topic_map=topic_map,
        event_writer=create_event,
        logger=logger,
    )
