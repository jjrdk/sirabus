import abc
import asyncio
from typing import Callable, List, Tuple

from aett.eventstore import BaseEvent
from aett.eventstore.base_command import BaseCommand

from sirabus import (
    IHandleEvents,
    IHandleCommands,
    CommandResponse,
    get_type_param,
    EndpointConfiguration,
)
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class ServiceBusConfiguration(EndpointConfiguration, abc.ABC):
    def __init__(
        self,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
    ):
        super().__init__()
        self._message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ] = message_reader
        self._command_response_writer: Callable[
            [CommandResponse], Tuple[str, bytes]
        ] = command_response_writer
        self._handlers: List[IHandleEvents | IHandleCommands] = []

    def get_handlers(self) -> List[IHandleEvents | IHandleCommands]:
        return self._handlers

    def read(self, headers: dict, body: bytes) -> Tuple[dict, BaseEvent | BaseCommand]:
        return self._message_reader(self._topic_map, headers, body)

    def write_response(self, response: CommandResponse) -> Tuple[str, bytes]:
        return self._command_response_writer(response)

    def with_handlers(self, *handlers: IHandleEvents | IHandleCommands):
        self._handlers.extend(handlers)
        return self


class ServiceBus[TConfiguration: ServiceBusConfiguration](abc.ABC):
    def __init__(self, configuration: TConfiguration) -> None:
        """
        Initializes the ServiceBus.
        :param configuration: The service bus configuration.
        :raises ValueError: If the message reader cannot determine the topic for the event or command.
        :raises TypeError: If the event or command is not a subclass of BaseEvent or BaseCommand.
        :raises Exception: If there is an error during message handling or response sending.
        :return: None
        """
        self._configuration: TConfiguration = configuration

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

    async def _pre_handle_message(
        self, headers: dict, body: bytes
    ) -> Tuple[dict, BaseEvent | BaseCommand]:
        return self._configuration.read(headers, body)

    async def _handle_message(
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
        headers, event = await self._pre_handle_message(headers, body)
        if isinstance(event, BaseEvent):
            await self._handle_event(event, headers)
        elif isinstance(event, BaseCommand):
            if not reply_to:
                raise RuntimeError(
                    f"Reply to field is empty for command {type(event)} with correlation ID {correlation_id}."
                )
            await self._handle_command(
                command=event,
                headers=headers,
                message_id=message_id,
                correlation_id=correlation_id,
                reply_to=reply_to,
            )
        elif isinstance(event, CommandResponse):
            pass
        else:
            raise TypeError(f"Unexpected message type: {type(event)}")

    async def _handle_command(
        self,
        command: BaseCommand,
        headers: dict,
        message_id: str | None,
        reply_to: str,
        correlation_id: str | None,
    ) -> None:
        topic_map = self._configuration.get_topic_map()
        command_type = type(command)
        command_handler = next(
            (
                h
                for h in self._configuration.get_handlers()
                if (
                    isinstance(h, IHandleCommands)
                    and topic_map.get_from_type(command_type)
                    == topic_map.get_from_type(get_type_param(h))
                )
            ),
            None,
        )
        if not command_handler:
            await self._send_command_response(
                response=CommandResponse(success=False, message="unknown command"),
                message_id=message_id,
                correlation_id=correlation_id,
                reply_to=reply_to,
            )
            return
        response = await command_handler.handle(command=command, headers=headers)
        await self._send_command_response(
            response=response,
            message_id=message_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

    async def _handle_event(self, event: BaseEvent, headers: dict) -> None:
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
                for h in self._configuration.get_handlers()
                if isinstance(h, IHandleEvents) and isinstance(event, get_type_param(h))
            ],
            return_exceptions=True,
        )
        self._configuration.get_logger().debug(
            "Event handled",
        )

    @abc.abstractmethod
    async def _send_command_response(
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
