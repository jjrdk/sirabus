import asyncio
import logging

import aio_pika
from aett.eventstore import BaseEvent
from aio_pika import Message

from sirabus import IPublishEvents, TEvent
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump


class AmqpCloudEventPublisher(IPublishEvents[TEvent]):
    """
    Publishes events as CloudEvents.
    """

    def __init__(
        self,
        amqp_url: str,
        topic_map: HierarchicalTopicMap,
        logger: logging.Logger | None = None,
    ) -> None:
        self.__amqp_url = amqp_url
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger("CloudEventPublisher")

    async def publish(self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic.
        :param event: The event to publish.
        """
        from sirabus.publisher import create_cloud_event

        topic, hierarchical_topic, j = create_cloud_event(event, self.__topic_map)

        connection = await aio_pika.connect_robust(url=self.__amqp_url)
        channel = await connection.channel()
        exchange = await channel.get_exchange(name="amq.topic", ensure=True)
        self.__logger.debug("Channel opened for publishing CloudEvent.")
        response = await exchange.publish(
            message=Message(body=j.encode(), headers={"topic": topic}),
            routing_key=hierarchical_topic,
        )
        self.__logger.debug(f"Published {response}")
        await channel.close()
        await connection.close()


class InMemoryCloudEventPublisher(IPublishEvents[TEvent]):
    """
    Publishes events as CloudEvents in memory.
    """

    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        messagepump: MessagePump,
        logger: logging.Logger | None = None,
    ) -> None:
        self.__topic_map = topic_map
        self.__messagepump = messagepump
        self.__logger = logger or logging.getLogger("InMemoryCloudEventPublisher")

    async def publish(self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic in memory.
        :param event: The event to publish.
        """
        from sirabus.publisher import create_cloud_event

        topic, hierarchical_topic, j = create_cloud_event(event, self.__topic_map)
        self.__messagepump.publish(({"topic": topic}, j.encode()))
        await asyncio.sleep(0)


def create_publisher_for_amqp_cloudevent(
    amqp_url: str, topic_map: HierarchicalTopicMap, logger: logging.Logger | None = None
) -> IPublishEvents[BaseEvent]:
    """
    Creates a CloudEventPublisher for AMQP.
    :param amqp_url: The AMQP URL.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    return AmqpCloudEventPublisher(
        amqp_url=amqp_url, topic_map=topic_map, logger=logger
    )


def create_publisher_for_memory_cloudevent(
    topic_map: HierarchicalTopicMap,
    messagepump: MessagePump,
    logger: logging.Logger | None = None,
) -> IPublishEvents[BaseEvent]:
    """
    Creates a CloudEventPublisher for in-memory use.
    :param topic_map: The hierarchical topic map.
    :param messagepump: The message pump for in-memory publishing.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    return InMemoryCloudEventPublisher(
        topic_map=topic_map, messagepump=messagepump, logger=logger
    )
