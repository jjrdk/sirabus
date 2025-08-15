import abc
import asyncio
import logging
from typing import Tuple, Callable, List

from aett.eventstore import BaseEvent
from aett.eventstore.base_command import BaseCommand

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class ServiceBus(abc.ABC):
    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ],
        handlers: List[IHandleEvents | IHandleCommands],
        logger: logging.Logger,
    ) -> None:
        """
        Initializes the ServiceBus.
        :param topic_map: The hierarchical topic map for topic resolution.
        :param message_reader: A callable that reads the message and returns headers and an event or command.
        :param handlers: A list of event and command handlers.
        :param logger: Optional logger for logging.
        :raises ValueError: If the message reader cannot determine the topic for the event or command.
        :raises TypeError: If the event or command is not a subclass of BaseEvent or BaseCommand.
        :raises Exception: If there is an error during message handling or response sending.
        :return: None
        """
        self._logger = logger
        self._topic_map = topic_map
        self._message_reader = message_reader
        self._handlers = handlers

    @abc.abstractmethod
    async def run(self):
        """
        Starts the service bus and begins processing messages.
        :raises RuntimeError: If the service bus cannot be started.
        :raises Exception: If there is an error during message processing.
        :return: None
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def stop(self):
        """
        Stops the service bus and cleans up resources.
        :raises RuntimeError: If the service bus cannot be stopped.
        :raises Exception: If there is an error during cleanup.
        :return: None
        """
        raise NotImplementedError()

    async def handle_message(
        self,
        headers: dict,
        body: bytes,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str | None,
    ) -> None:
        """
        Handles a message by reading it and dispatching it to the appropriate handler.
        :param headers: The headers of the message.
        :param body: The body of the message.
        :param message_id: The ID of the message.
        :param correlation_id: The correlation ID of the message.
        :param reply_to: The reply-to address for the message.
        :raises ValueError: If the topic is not found in the topic map.
        :raises TypeError: If the event or command type is not a subclass of BaseEvent or BaseCommand.
        :raises Exception: If there is an error during message handling or response sending.
        :return: None
        """
        headers, event = self._message_reader(self._topic_map, headers, body)
        if isinstance(event, BaseEvent):
            await self.handle_event(event, headers)
        elif isinstance(event, BaseCommand):
            command_handler = next(
                (
                    h
                    for h in self._handlers
                    if isinstance(h, IHandleCommands)
                    and self._topic_map.get_from_type(type(event))
                    == self._topic_map.get_from_type(get_type_param(h))
                ),
                None,
            )
            if not command_handler:
                if not reply_to:
                    self._logger.error(
                        f"No command handler found for command {type(event)} with correlation ID {correlation_id} "
                        f"and no reply_to field provided."
                    )
                    return
                await self.send_command_response(
                    response=CommandResponse(success=False, message="unknown command"),
                    message_id=message_id,
                    correlation_id=correlation_id,
                    reply_to=reply_to,
                )
                return
            response = await command_handler.handle(command=event, headers=headers)
            if not reply_to:
                self._logger.error(
                    f"Reply to field is empty for command {type(event)} with correlation ID {correlation_id}."
                )
                return
            await self.send_command_response(
                response=response,
                message_id=message_id,
                correlation_id=correlation_id,
                reply_to=reply_to,
            )
        elif isinstance(event, CommandResponse):
            pass
        else:
            raise TypeError(f"Unexpected message type: {type(event)}")

    async def handle_event(self, event: BaseEvent, headers: dict) -> None:
        """
        Handles an event by dispatching it to all registered event handlers that can handle the event type.
        :param event: The event to handle.
        :param headers: Additional headers associated with the event.
        :raises ValueError: If the event type is not found in the topic map.
        :raises TypeError: If the event type is not a subclass of BaseEvent.
        :raises Exception: If there is an error during event handling.
        :return: None
        """
        await asyncio.gather(
            *[
                h.handle(event=event, headers=headers)
                for h in self._handlers
                if isinstance(h, IHandleEvents) and isinstance(event, get_type_param(h))
            ],
            return_exceptions=True,
        )
        self._logger.debug(
            "Event handled",
        )

    @abc.abstractmethod
    async def send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ) -> None:
        """
        Sends a command response to the specified reply-to address.
        :param response: The command response to send.
        :param message_id: The ID of the original message.
        :param correlation_id: The correlation ID of the original message.
        :param reply_to: The reply-to address for the command response.
        :raises ValueError: If the reply_to address is not provided.
        :raises TypeError: If the response type is not a subclass of CommandResponse.
        :raises Exception: If there is an error during command response sending.
        :return: None
        """
        pass


def create_servicebus_for_amqp_pydantic(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    event_handlers: List[IHandleEvents | IHandleCommands],
    logger=None,
    prefetch_count=10,
):
    """
    Create a ServiceBus instance for AMQP using Pydantic serialization.
    :param amqp_url: The AMQP URL for the service bus.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param event_handlers: A list of event and command handlers.
    :param logger: Optional logger for logging.
    :param prefetch_count: The number of messages to prefetch from the service bus.
    :return: An instance of AmqpServiceBus.
    :raises ValueError: If the topic map is not provided.
    :raises TypeError: If the event handlers are not instances of IHandleEvents or IHandleCommands.
    :raises Exception: If there is an error during service bus creation.
    """
    from sirabus.servicebus.amqp_servicebus import AmqpServiceBus
    from sirabus.publisher.pydantic_serialization import read_event_message
    from sirabus.publisher.pydantic_serialization import create_command_response

    return AmqpServiceBus(
        amqp_url=amqp_url,
        topic_map=topic_map,
        handlers=event_handlers,
        message_reader=read_event_message,
        command_response_writer=create_command_response,
        logger=logger,
        prefetch_count=prefetch_count,
    )
