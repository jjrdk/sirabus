import abc
import asyncio
import logging
import uuid
from threading import Thread
from typing import Tuple

from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractQueue, AbstractIncomingMessage
from cloudevents.pydantic import CloudEvent

from sirabus import IRouteCommands, TCommand, CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class CommandRouter(IRouteCommands, abc.ABC):
    def __init__(
            self,
            amqp_url: str,
            topic_map: HierarchicalTopicMap,
            logger: logging.Logger | None = None,
    ) -> None:
        self.__amqp_url = amqp_url
        self._topic_map = topic_map
        self._logger = logger or logging.getLogger("CloudEventRouter")

    async def route(self, command: TCommand) -> CommandResponse:
        connection = await connect_robust(url=self.__amqp_url)
        channel = await connection.channel()
        response_queue: AbstractQueue = await channel.declare_queue(
            name=str(uuid.uuid4()), durable=False, exclusive=True, auto_delete=True
        )
        topic, hierarchical_topic, j = self._create_message(command, response_queue=response_queue.name)
        exchange = await channel.get_exchange(name=hierarchical_topic, ensure=True)
        self._logger.debug("Channel opened for publishing CloudEvent.")
        response = await exchange.publish(
            message=Message(body=j.encode(),
                            headers={"topic": topic},
                            correlation_id=command.correlation_id,
                            content_encoding="utf-8",
                            content_type="application/json"),
            routing_key=hierarchical_topic,
        )
        self._logger.debug(f"Published {response}")

        try:
            async for message in response_queue.iterator():
                self._logger.debug(f"Received response message: {message}")
                command_response = self._read_response(response_task)
                return command_response
        except Exception as e:
            self._logger.exception(f"Error while waiting for response: {e}", exc_info=e)
            return CommandResponse(success=False, message=str(e))
        finally:
            await channel.close()
            await connection.close()

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


class CloudEventRouter(CommandRouter):
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
