import asyncio
import logging
import uuid
from typing import Dict, Tuple, Optional, Callable

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


class AmqpCommandRouter(IRouteCommands):
    """AMQP Command Router for handling commands and responses over AMQP.
    This class implements the IRouteCommands interface to route commands
    using AMQP protocol. It manages the connection to the AMQP broker,
    publishes commands, and consumes responses asynchronously.
    """

    def __init__(
        self,
        amqp_url: str,
        topic_map: HierarchicalTopicMap,
        message_writer: Callable[
            [BaseCommand, HierarchicalTopicMap], Tuple[str, str, str]
        ],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initializes the AmqpCommandRouter with the necessary parameters.
        :param amqp_url: The URL of the AMQP broker.
        :param topic_map: The hierarchical topic map for routing commands.
        :param message_writer: A callable that formats the command into a message.
        :param response_reader: A callable that reads the response from the message.
        :param logger: Optional logger for logging debug information.
        :raises ValueError: If the amqp_url is empty or if the topic_map is None.
        :raises TypeError: If message_writer or response_reader are not callable.
        """
        self._response_reader = response_reader
        self._message_writer = message_writer
        self.__inflight: Dict[
            str, Tuple[asyncio.Future[CommandResponse], AbstractChannel]
        ] = {}
        self.__amqp_url = amqp_url
        self.__connection: Optional[AbstractRobustConnection] = None
        self._topic_map = topic_map
        self._logger = logger or logging.getLogger("AmqpCommandRouter")

    async def _get_connection(self) -> AbstractRobustConnection:
        if self.__connection is None or self.__connection.is_closed:
            self.__connection = await connect_robust(url=self.__amqp_url)
        return self.__connection

    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        loop = asyncio.get_event_loop()
        try:
            _, hierarchical_topic, j = self._message_writer(command, self._topic_map)
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
        self._logger.debug("Channel opened for publishing CloudEvent.")
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
        self._logger.debug(f"Published {response}")
        future = loop.create_future()
        self.__inflight[consume_tag] = (future, channel)
        return future

    async def _consume_queue(self, msg: AbstractIncomingMessage) -> None:
        if msg.consumer_tag is None:
            self._logger.error(
                "Message received without consumer tag, cannot process response."
            )
            return
        future, channel = self.__inflight[msg.consumer_tag]
        response = (
            CommandResponse(success=False, message="No response received.")
            if not msg
            else self._response_reader(msg.headers, msg.body)
        )
        if response is not None:
            future.set_result(response)
            await channel.close()
        else:
            self._logger.error("Could not read response from message.")
