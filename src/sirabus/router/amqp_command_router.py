import asyncio
import uuid
from typing import Dict, Tuple, Optional, Callable, Self

from aett.eventstore.base_command import BaseCommand
from aio_pika import connect_robust, Message
from aio_pika.abc import (
    AbstractChannel,
    AbstractRobustConnection,
    AbstractQueue,
    AbstractIncomingMessage,
)

from sirabus import CommandResponse, IRouteCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.router import RouterConfiguration


class AmqpRouterConfiguration(RouterConfiguration):
    """Configuration for AMQP Command Router."""

    def __init__(
        self,
        message_writer: Callable[[BaseCommand, HierarchicalTopicMap], Tuple[str, str]],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
    ) -> None:
        """
        Initializes the AmqpRouterConfiguration with the necessary parameters.
        :param Callable message_writer: A callable that formats the command into a message.
        :param Callable response_reader: A callable that reads the response from the message.
        :raises ValueError: If the amqp_url is empty.
        """
        super().__init__(message_writer=message_writer, response_reader=response_reader)
        self._amqp_url: Optional[str] = None
        self._message_writer: Callable[
            [BaseCommand, HierarchicalTopicMap], Tuple[str, str]
        ] = message_writer
        self._response_reader: Callable[[dict, bytes], CommandResponse | None] = (
            response_reader
        )

    def get_amqp_url(self) -> str:
        """
        Gets the AMQP URL.
        :return: The AMQP URL.
        :raises ValueError: If the amqp_url is empty.
        """
        if not self._amqp_url:
            raise ValueError("amqp_url has not been set.")
        return self._amqp_url

    def with_amqp_url(self, amqp_url: str) -> Self:
        """
        Sets the AMQP URL.
        :param str amqp_url: The AMQP URL to set.
        :return: The AmqpRouterConfiguration instance.
        :raises ValueError: If the amqp_url is empty.
        """
        if not amqp_url:
            raise ValueError("amqp_url cannot be empty.")
        self._amqp_url = amqp_url
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            write_command,
            read_command_response,
        )

        return AmqpRouterConfiguration(write_command, read_command_response)

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            write_command,
            read_command_response,
        )

        return AmqpRouterConfiguration(write_command, read_command_response)


class AmqpCommandRouter(IRouteCommands):
    """AMQP Command Router for handling commands and responses over AMQP.
    This class implements the IRouteCommands interface to route commands
    using AMQP protocol. It manages the connection to the AMQP broker,
    publishes commands, and consumes responses asynchronously.
    """

    def __init__(self, configuration: AmqpRouterConfiguration) -> None:
        """
        Initializes the AmqpCommandRouter with the necessary parameters.
        :param AmqpRouterConfiguration configuration: The configuration object containing AMQP URL, topic map,
        :raises ValueError: If the amqp_url is empty or if the topic_map is None.
        :raises TypeError: If message_writer or response_reader are not callable.
        """
        if not configuration.get_amqp_url():
            raise ValueError("amqp_url cannot be empty.")
        self._configuration = configuration
        self.__inflight: Dict[
            str, Tuple[asyncio.Future[CommandResponse], AbstractChannel]
        ] = {}
        self.__connection: Optional[AbstractRobustConnection] = None

    async def _get_connection(self) -> AbstractRobustConnection:
        if self.__connection is None or self.__connection.is_closed:
            self.__connection = await connect_robust(
                url=self._configuration.get_amqp_url(),
                ssl=(self._configuration.get_ssl_config() is not None),
                ssl_context=self._configuration.get_ssl_config(),
            )
        return self.__connection

    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        loop = asyncio.get_event_loop()
        try:
            hierarchical_topic, j = self._configuration.write_message(command)
        except ValueError:
            future = loop.create_future()
            future.set_result(CommandResponse(success=False, message="unknown command"))
            return future
        connection = await self._get_connection()
        channel = await connection.channel()
        response_queue: AbstractQueue = await channel.declare_queue(
            name=str(uuid.uuid4()), durable=False, exclusive=True, auto_delete=True
        )
        consume_tag = await response_queue.consume(callback=self._consume_queue)
        exchange = await channel.get_exchange(name="amq.topic", ensure=False)
        self._configuration.get_logger().debug(
            "Channel opened for publishing CloudEvent."
        )
        response = await exchange.publish(
            message=Message(
                body=j.encode(),
                headers={"topic": hierarchical_topic},
                correlation_id=command.correlation_id,
                content_encoding="utf-8",
                content_type="application/json",
                reply_to=response_queue.name,
            ),
            routing_key=hierarchical_topic,
        )
        self._configuration.get_logger().debug(f"Published {response}")
        future = loop.create_future()
        self.__inflight[consume_tag] = (future, channel)
        return future

    async def _consume_queue(self, msg: AbstractIncomingMessage) -> None:
        if msg.consumer_tag is None:
            self._configuration.get_logger().error(
                "Message received without consumer tag, cannot process response."
            )
            return
        future, channel = self.__inflight[msg.consumer_tag]
        response = (
            CommandResponse(success=False, message="No response received.")
            if not msg
            else self._configuration.read_response(msg.headers, msg.body)
        )
        if response is not None:
            future.set_result(response)
            await channel.close()
        else:
            self._configuration.get_logger().error(
                "Could not read response from message."
            )
