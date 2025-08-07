import logging

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.publisher.amqp_publisher import AmqpPublisher
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
    from sirabus.publisher.cloudevent_serialization import create_event

    return AmqpPublisher(
        amqp_url=amqp_url,
        topic_map=topic_map,
        logger=logger,
        event_writer=create_event,
    )


def create_publisher_for_inmemory(
    topic_map: HierarchicalTopicMap,
    message_pump: MessagePump,
    logger: logging.Logger | None = None,
) -> IPublishEvents:
    """
    Creates a CloudEventPublisher for in-memory use.
    :param topic_map: The hierarchical topic map.
    :param message_pump: The message pump for in-memory publishing.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    from sirabus.publisher.cloudevent_serialization import create_event

    return InMemoryPublisher(
        topic_map=topic_map,
        messagepump=message_pump,
        logger=logger,
        event_writer=create_event,
    )
