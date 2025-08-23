import logging
from typing import List, Optional, Set, Callable, Tuple

import aio_pika
from aett.eventstore import BaseEvent
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustConnection,
    AbstractRobustChannel,
)

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus


class AmqpServiceBus(ServiceBus):
    """
    An implementation of the ServiceBus that uses AMQP (Advanced Message Queuing Protocol)
    for communication with RabbitMQ. This class is designed to handle both events and commands
    using a hierarchical topic map for routing messages.
    It supports asynchronous message handling and allows for command responses to be sent back
    to the requester.
    :param amqp_url: The AMQP URL to connect to RabbitMQ.
    :param topic_map: The topic map to use for resolving topics.
    :param handlers: The list of event and command handlers to register.
    :param message_reader: A callable that reads messages from the topic map and returns a tuple
                         containing the headers and the BaseEvent instance.
    :param command_response_writer: A callable that writes command responses to the appropriate topic.
    :param prefetch_count: The number of messages to prefetch from RabbitMQ.
    :param logger: An optional logger instance for logging messages.
    :type amqp_url: str
    :type topic_map: HierarchicalTopicMap
    :type handlers: List[IHandleEvents | IHandleCommands]
    :type message_reader: Callable[[HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]]
    :type command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]]
    :type prefetch_count: int
    :type logger: Optional[logging.Logger]
    :raises ValueError: If the AMQP URL is invalid or if no handlers are provided.
    :raises RuntimeError: If the connection to RabbitMQ cannot be established.
    :raises Exception: For any other exceptions that occur during message handling.
    :note: This class is designed to be used in an asynchronous context, and it requires
           an event loop to run the `run` method. The `stop` method should be called to
           gracefully shut down the service bus and close the connection to RabbitMQ.
    :example:
        >>> from sirabus import AmqpServiceBus, HierarchicalTopicMap, IHandleEvents
        >>> import asyncio
        >>> class MyEventHandler(IHandleEvents):
        ...     async def handle(self, event, headers):
        ...         print(f"Handling event: {event} with headers: {headers}")
        >>> topic_map = HierarchicalTopicMap()
        >>> handlers = [MyEventHandler()]
        >>> amqp_url = "amqp://guest:guest@localhost/"
        >>> message_reader = lambda topic_map, headers, body: (headers, BaseEvent.from_json(body))
        >>> command_response_writer = lambda response: ("response_topic", response.to_json().encode('utf-8'))
        >>> service_bus = AmqpServiceBus(
        ...     amqp_url=amqp_url,
        ...     topic_map=topic_map,
        ...     handlers=handlers,
        ...     message_reader=message_reader,
        ...     command_response_writer=command_response_writer,
        ...     prefetch_count=10,
        ... )
        >>> asyncio.run(service_bus.run())
    :note: The `run` method starts the service bus and begins consuming messages from RabbitMQ.
           The `stop` method should be called to gracefully shut down the service bus and close
           the connection to RabbitMQ.
    """

    def __init__(
        self,
        amqp_url: str,
        topic_map: HierarchicalTopicMap,
        handlers: List[IHandleEvents | IHandleCommands],
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
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
            message_reader=message_reader,
            topic_map=topic_map,
            handlers=handlers,
            logger=logger or logging.getLogger("AmqpServiceBus"),
        )
        self.__command_response_writer = command_response_writer
        self.__amqp_url = amqp_url
        self._topic_map = topic_map
        self._prefetch_count = prefetch_count
        self._logger = logger or logging.getLogger("ServiceBus")
        self.__topics = set(
            topic
            for topic in (
                self._topic_map.get_from_type(get_type_param(handler))
                for handler in handlers
                if isinstance(handler, (IHandleEvents, IHandleCommands))
            )
            if topic is not None
        )
        self.__queue_name = self._get_consumer_queue_name(self.__topics)
        self.__connection: Optional[AbstractRobustConnection] = None
        self.__channel: Optional[AbstractRobustChannel] = None
        self.__consumer_tag: Optional[str] = None

    async def __inner_handle_message(self, msg: AbstractIncomingMessage):
        try:
            await self.handle_message(
                headers=msg.headers,
                body=msg.body,
                message_id=msg.message_id,
                correlation_id=msg.correlation_id,
                reply_to=msg.reply_to,
            )
            await msg.ack()
        except Exception as e:
            self._logger.exception("Exception while handling message", exc_info=e)
            await msg.nack(requeue=True)

    async def run(self):
        self._logger.debug("Starting service bus")
        self.__connection = await aio_pika.connect_robust(url=self.__amqp_url)
        self.__channel = await self.__connection.channel()
        await self.__channel.set_qos(prefetch_count=self._prefetch_count)
        self._logger.debug("Channel opened for consuming messages.")
        queue = await self.__channel.declare_queue(self.__queue_name, exclusive=True)
        for topic in self.__topics:
            await queue.bind(exchange=topic, routing_key=f"{topic}.#")
            self._logger.debug(f"Queue {self.__queue_name} bound to topic {topic}.")
        self.__consumer_tag = await queue.consume(callback=self.__inner_handle_message)

    async def stop(self):
        if self.__consumer_tag:
            queue = await self.__channel.get_queue(self.__queue_name)
            await queue.cancel(self.__consumer_tag)
            await self.__channel.close()
            await self.__connection.close()

    async def send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ):
        if not self.__channel or self.__channel.is_closed:
            return
        topic, j = self.__command_response_writer(response)
        await self.__channel.default_exchange.publish(
            aio_pika.Message(
                body=j,
                correlation_id=correlation_id,
                content_type="application/json",
                content_encoding="utf-8",
            ),
            routing_key=reply_to,
        )
        self._logger.debug(
            f"Response published to {reply_to} with correlation_id {correlation_id}."
        )

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
