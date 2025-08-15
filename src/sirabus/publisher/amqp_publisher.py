import logging
from typing import Callable, Tuple

from aio_pika import connect_robust, Message
from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class AmqpPublisher(IPublishEvents):
    """
    Publishes events over AMQP.
    """

    def __init__(
        self,
        amqp_url: str,
        topic_map: HierarchicalTopicMap,
        event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str, str]],
        logger: logging.Logger | None = None,
    ) -> None:
        self._event_writer = event_writer
        self.__amqp_url = amqp_url
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger("AmqpPublisher")

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic.
        :param event: The event to publish.
        """

        _, hierarchical_topic, j = self._event_writer(event, self.__topic_map)

        connection = await connect_robust(url=self.__amqp_url)
        channel = await connection.channel()
        exchange = await channel.get_exchange(name="amq.topic", ensure=False)
        self.__logger.debug("Channel opened for publishing event.")
        response = await exchange.publish(
            message=Message(body=j.encode(), headers={"topic": hierarchical_topic}),
            routing_key=hierarchical_topic,
        )
        self.__logger.debug(f"Published {response}")
        await channel.close()
        await connection.close()
