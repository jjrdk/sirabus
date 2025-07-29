import logging
from typing import List, Optional, Set, Callable, Tuple

import aio_pika
from aett.eventstore import BaseEvent
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustConnection,
    AbstractRobustChannel,
)

from sirabus import IHandleEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus


class AmqpServiceBus(ServiceBus):
    """
    Base class for event handlers.
    """

    def __init__(
        self,
        amqp_url: str,
        topic_map: HierarchicalTopicMap,
        handlers: List[IHandleEvents],
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]
        ],
        prefetch_count: int = 10,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param str amqp_url: The AMQP URL to connect to RabbitMQ.
        :param HierarchicalTopicMap topic_map: The topic map to use for resolving topics.
        :param List[IHandleEvents] handlers: The list of event handlers to register.
        :param int prefetch_count: The number of messages to prefetch from RabbitMQ.
        """
        super().__init__(
            message_reader=message_reader, topic_map=topic_map, handlers=handlers
        )
        self.__handlers: List[IHandleEvents] = [handler for handler in handlers]
        self.__amqp_url = amqp_url
        self._topic_map = topic_map
        self._prefetch_count = prefetch_count
        self._logger = logger or logging.getLogger("ServiceBus")
        self.__topics = set(
            topic
            for topic in (
                self._topic_map.get_hierarchical_topic(type(handler).event_type)
                for handler in handlers
                if isinstance(handler, IHandleEvents)
            )
            if topic is not None
        )
        self.__queue_name = self._get_consumer_queue_name(self.__topics)
        self.__connection: Optional[AbstractRobustConnection] = None
        self.__channel: Optional[AbstractRobustChannel] = None
        self.__consumer_tag: Optional[str] = None

    async def __inner_handle_message(self, msg: AbstractIncomingMessage):
        try:
            await self.handle_event(msg.headers, msg.body)
            await msg.ack()
        except Exception as e:
            logging.exception("Exception while handling message", exc_info=e)
            await msg.nack(requeue=True)

    async def run(self):
        logging.debug("Starting service bus")
        self.__connection = await aio_pika.connect_robust(url=self.__amqp_url)
        self.__channel = await self.__connection.channel()
        await self.__channel.set_qos(prefetch_count=self._prefetch_count)
        logging.debug("Channel opened for consuming messages.")
        queue = await self.__channel.declare_queue(self.__queue_name, exclusive=True)
        for topic in self.__topics:
            await queue.bind(exchange=topic, routing_key=topic)
            logging.debug(f"Queue {self.__queue_name} bound to topic {topic}.")
        self.__consumer_tag = await queue.consume(callback=self.__inner_handle_message)

    async def stop(self):
        if self.__consumer_tag:
            queue = await self.__channel.get_queue(self.__queue_name)
            await queue.cancel(self.__consumer_tag)
            await self.__channel.close()
            await self.__connection.close()

    @staticmethod
    def _get_consumer_queue_name(topics: Set[str]) -> str:
        """
        Returns the queue name for the given topic.
        :param topics: The topics for which to get the queue name.
        :return: The queue name.
        """
        import hashlib

        h = hashlib.sha256(usedforsecurity=False)
        for topic in topics:
            h.update(topic.encode())
        hashed_topics = h.hexdigest()
        return hashed_topics
