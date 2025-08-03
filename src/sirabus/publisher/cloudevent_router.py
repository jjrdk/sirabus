import abc
import asyncio
import logging
import uuid
from typing import Dict, Tuple, Optional

from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractQueue, AbstractIncomingMessage, AbstractRobustConnection, AbstractChannel
from cloudevents.pydantic import CloudEvent

from sirabus import IRouteCommands, TCommand, CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class AmqpCommandRouter(IRouteCommands, abc.ABC):
    def __init__(
            self,
            amqp_url: str,
            topic_map: HierarchicalTopicMap,
            logger: logging.Logger | None = None,
    ) -> None:
        self.__inflight: Dict[str, Tuple[asyncio.Future[CommandResponse], AbstractChannel]] = {}
        self.__amqp_url = amqp_url
        self.__connection: Optional[AbstractRobustConnection] = None
        self._topic_map = topic_map
        self._logger = logger or logging.getLogger("CloudEventRouter")

    async def _get_connection(self) -> AbstractRobustConnection:
        if self.__connection is None or self.__connection.is_closed:
            self.__connection = await connect_robust(url=self.__amqp_url)
        return self.__connection

    async def route(self, command: TCommand) -> asyncio.Future[CommandResponse]:
        loop = asyncio.get_event_loop()
        connection = await self._get_connection()
        channel = await connection.channel()
        response_queue: AbstractQueue = await channel.declare_queue(
            name=str(uuid.uuid4()), durable=False, exclusive=True, auto_delete=True
        )
        consume_tag = await response_queue.consume(callback=self._consume_queue)
        try:
            topic, hierarchical_topic, j = self._create_message(command, response_queue=response_queue.name)
        except ValueError as ve:
            self._logger.exception(f"Error creating message for command {command}: {ve}")
            future =loop.create_future()
            future.set_result(CommandResponse(success=False, message="unknown command"))
            return future
        exchange = await channel.get_exchange(name="amq.topic", ensure=False)
        self._logger.debug("Channel opened for publishing CloudEvent.")
        response = await exchange.publish(
            message=Message(body=j.encode(),
                            headers={"topic": topic},
                            correlation_id=command.correlation_id,
                            content_encoding="utf-8",
                            content_type="application/json",
                            reply_to=response_queue.name),
            routing_key=hierarchical_topic,
        )
        self._logger.debug(f"Published {response}")
        future = loop.create_future()
        self.__inflight[consume_tag] = (future, channel)
        return future

    async def _consume_queue(self, msg: AbstractIncomingMessage) -> None:
        if msg.consumer_tag is None:
            self._logger.error("Message received without consumer tag, cannot process response.")
            return
        future, channel = self.__inflight[msg.consumer_tag]
        response = self._read_response(msg)
        future.set_result(response)
        await channel.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for future, channel in self.__inflight.values():
            if not future.done():
                future.cancel("Command routing was cancelled due to shutdown.")
            if not channel.is_closed:
                await channel.close()
        await self.__connection.close()

    @abc.abstractmethod
    def _create_message(
            self, command: TCommand, response_queue: str
    ) -> Tuple[str, str, str]:
        """
        Create a message to be sent over the AMQP channel.
        :param command: The command to be sent.
        :return: An aio_pika Message object.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _read_response(self, response_msg: AbstractIncomingMessage | None) -> CommandResponse:
        raise NotImplementedError()


class CloudEventRouter(AmqpCommandRouter):
    def _create_message(self, command: TCommand, response_queue: str) -> Tuple[str, str, str]:
        from sirabus.publisher import create_cloud_command
        return create_cloud_command(
            command=command, topic_map=self._topic_map, reply_to=response_queue
        )

    def _read_response(self, response_msg: AbstractIncomingMessage | None) -> CommandResponse:
        """
        Reads the response message and returns a CommandResponse.
        :param response_msg: The response message received from the command.
        :return: A CommandResponse indicating the success or failure of the command routing.
        """
        if not response_msg:
            return CommandResponse(success=False, message="No response received.")

        try:
            cloud_event = CloudEvent.model_validate_json(response_msg.body)
            response = CommandResponse.model_validate(cloud_event.data)
            return response
        except Exception as e:
            self._logger.exception(f"Error processing response: {e}", exc_info=e)
            return CommandResponse(success=False, message=str(e))
