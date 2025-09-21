from typing import Callable, Optional, Tuple

import aio_pika
from aett.eventstore import BaseEvent, BaseCommand
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustConnection,
    AbstractRobustChannel,
)

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBusConfiguration, ServiceBus


class AmqpServiceBusConfiguration(ServiceBusConfiguration):
    def __init__(
        self,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
    ):
        super().__init__(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )
        self._amqp_url: Optional[str] = None
        self._prefetch_count: int = 10
        import uuid

        self._receive_endpoint_name: str = str(uuid.uuid4())

    def get_amqp_url(self) -> Optional[str]:
        """
        Get the AMQP URL.
        :return: The AMQP URL.
        :rtype: str
        :raises ValueError: If the AMQP URL is not set.
        """
        return self._amqp_url

    def get_prefetch_count(self) -> int:
        """
        Get the prefetch count.
        :return: The prefetch count.
        :rtype: int
        :raises ValueError: If the prefetch count is less than 1.
        """
        return self._prefetch_count

    def get_receive_endpoint_name(self) -> str:
        """
        Get the receive endpoint name.
        :return: The receive endpoint name.
        :rtype: str
        :raises ValueError: If the receive endpoint name is not set.
        """
        return self._receive_endpoint_name

    def with_amqp_url(self, amqp_url: str):
        """
        Set the AMQP URL.
        :param amqp_url: The AMQP URL.
        :return: The AMQP service bus configuration.
        :rtype: AmqpServiceBusConfiguration
        :raises ValueError: If the AMQP URL is empty.
        """
        self._amqp_url = amqp_url
        return self

    def with_prefetch_count(self, prefetch_count: int):
        """
        Set the prefetch count.
        :param prefetch_count: The prefetch count.
        :return: The AMQP service bus configuration.
        :rtype: AmqpServiceBusConfiguration
        :raises ValueError: If the prefetch count is less than 1.
        """
        if prefetch_count < 1:
            raise ValueError("prefetch_count must be >= 1")
        self._prefetch_count = prefetch_count
        return self

    def with_receive_endpoint_name(self, receive_endpoint_name: str):
        """
        Set the receive endpoint name.
        :param receive_endpoint_name: The receive endpoint name.
        :return: The AMQP service bus configuration.
        :rtype: AmqpServiceBusConfiguration
        :raises ValueError: If the receive endpoint name is empty.
        """
        if not receive_endpoint_name or receive_endpoint_name == "":
            raise ValueError("receive_endpoint_name must not be empty")
        self._receive_endpoint_name = receive_endpoint_name
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            read_event,
            write_command_response,
        )

        return AmqpServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            read_event,
            write_command_response,
        )

        return AmqpServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_custom(message_reader, command_response_writer):
        return AmqpServiceBusConfiguration(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )


class AmqpServiceBus(ServiceBus[AmqpServiceBusConfiguration]):
    """
    An implementation of the ServiceBus that uses AMQP (Advanced Message Queuing Protocol)
    for communication with RabbitMQ. This class is designed to handle both events and commands
    using a hierarchical topic map for routing messages.
    It supports asynchronous message handling and allows for command responses to be sent back
    to the requester.
    :note: This class is designed to be used in an asynchronous context, and it requires
           an event loop to run the `run` method. The `stop` method should be called to
           gracefully shut down the service bus and close the connection to RabbitMQ.
    :note: The `run` method starts the service bus and begins consuming messages from RabbitMQ.
           The `stop` method should be called to gracefully shut down the service bus and close
           the connection to RabbitMQ.
    """

    def __init__(self, configuration: AmqpServiceBusConfiguration) -> None:
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param AmqpServiceBusConfiguration configuration: The AMQP service bus configuration.
        """
        super().__init__(
            configuration=configuration,
        )
        self.__topics = set(
            topic
            for topic in (
                self._configuration.get_topic_map().get_from_type(
                    get_type_param(handler)
                )
                for handler in configuration.get_handlers()
                if isinstance(handler, (IHandleEvents, IHandleCommands))
            )
            if topic is not None
        )
        self.__connection: Optional[AbstractRobustConnection] = None
        self.__channel: Optional[AbstractRobustChannel] = None
        self.__consumer_tag: Optional[str] = None

    async def __inner_handle_message(self, msg: AbstractIncomingMessage):
        try:
            await self._handle_message(
                headers=msg.headers,
                body=msg.body,
                message_id=msg.message_id,
                correlation_id=msg.correlation_id,
                reply_to=msg.reply_to,
            )
            await msg.ack()
        except Exception as e:
            self._configuration.get_logger().exception(
                "Exception while handling message", exc_info=e
            )
            await msg.nack(requeue=True)

    async def run(self):
        self._configuration.get_logger().debug("Starting service bus")
        ssl_context = self._configuration.get_ssl_config()
        self.__connection = await aio_pika.connect_robust(
            url=self._configuration.get_amqp_url(),
            ssl=(ssl_context is not None),
            ssl_context=ssl_context,
        )
        self.__channel = await self.__connection.channel()
        await self.__channel.set_qos(
            prefetch_count=self._configuration.get_prefetch_count()
        )
        self._configuration.get_logger().debug("Channel opened for consuming messages.")
        queue = await self.__channel.declare_queue(
            self._configuration.get_receive_endpoint_name(), exclusive=True
        )
        for topic in self.__topics:
            await queue.bind(exchange=topic, routing_key=f"{topic}.#")
            self._configuration.get_logger().debug(
                f"Queue {self._configuration.get_receive_endpoint_name} bound to topic {topic}."
            )
        self.__consumer_tag = await queue.consume(callback=self.__inner_handle_message)

    async def stop(self):
        if self.__consumer_tag:
            queue = await self.__channel.get_queue(
                self._configuration.get_receive_endpoint_name()
            )
            await queue.cancel(self.__consumer_tag)
            await self.__channel.close()
            await self.__connection.close()

    async def _send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ):
        if not self.__channel or self.__channel.is_closed:
            return
        topic, j = self._configuration.write_response(response)
        await self.__channel.default_exchange.publish(
            aio_pika.Message(
                body=j,
                correlation_id=correlation_id,
                content_type="application/json",
                content_encoding="utf-8",
            ),
            routing_key=reply_to,
        )
        self._configuration.get_logger().debug(
            f"Response published to {reply_to} with correlation_id {correlation_id}."
        )
