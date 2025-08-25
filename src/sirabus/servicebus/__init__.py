import abc
import asyncio
import logging
import uuid
from typing import Callable, List, Tuple, Optional

from aett.eventstore import BaseEvent
from aett.eventstore.base_command import BaseCommand

from sirabus import (
    IHandleEvents,
    IHandleCommands,
    CommandResponse,
    get_type_param,
    SqsConfig,
)
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class ServiceBusConfiguration(abc.ABC):
    def __init__(
        self,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
    ):
        self._topic_map = HierarchicalTopicMap()
        self._message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ] = message_reader
        self._command_response_writer: Callable[
            [CommandResponse], Tuple[str, bytes]
        ] = command_response_writer
        self._handlers: List[IHandleEvents | IHandleCommands] = []
        self._logger = logging.getLogger("ServiceBus")

    def get_topic_map(self) -> HierarchicalTopicMap:
        return self._topic_map

    def get_handlers(self) -> List[IHandleEvents | IHandleCommands]:
        return self._handlers

    def get_logger(self) -> logging.Logger:
        return self._logger

    def read(self, headers: dict, body: bytes) -> Tuple[dict, BaseEvent | BaseCommand]:
        return self._message_reader(self._topic_map, headers, body)

    def write_response(self, response: CommandResponse) -> Tuple[str, bytes]:
        return self._command_response_writer(response)

    def with_topic_map(self, topic_map: HierarchicalTopicMap):
        self._topic_map = topic_map
        return self

    def with_handlers(self, *handlers: IHandleEvents | IHandleCommands):
        self._handlers.extend(handlers)
        return self

    def with_logger(self, logger: logging.Logger):
        self._logger = logger
        return self

    @abc.abstractmethod
    def build(self): ...

    @staticmethod
    @abc.abstractmethod
    def default(): ...

    @staticmethod
    @abc.abstractmethod
    def for_cloud_event(): ...


class RedisServiceBusConfiguration(ServiceBusConfiguration):
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
        self._redis_url: Optional[str] = None

    def get_redis_url(self) -> str:
        if not self._redis_url:
            raise ValueError("Redis URL is not set.")
        return self._redis_url

    def with_redis_url(self, redis_url: str):
        if not redis_url or redis_url == "":
            raise ValueError("redis_url must not be empty")
        self._redis_url = redis_url
        return self

    def build(self):
        if not self._redis_url:
            raise ValueError("Redis URL must be set before building the service bus.")
        from sirabus.servicebus.redis_servicebus import RedisServiceBus

        return RedisServiceBus(configuration=self)

    @staticmethod
    def default():
        from sirabus.publisher.pydantic_serialization import (
            read_event_message,
            create_command_response,
        )

        return RedisServiceBusConfiguration(
            message_reader=read_event_message,
            command_response_writer=create_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.publisher.cloudevent_serialization import (
            read_cloudevent_message,
            create_command_response,
        )

        return RedisServiceBusConfiguration(
            message_reader=read_cloudevent_message,
            command_response_writer=create_command_response,
        )


class SqsServiceBusConfiguration(ServiceBusConfiguration):
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
        self._sqs_config: Optional[SqsConfig] = None
        self._prefetch_count: int = 10
        self._receive_endpoint_name: str = "sqs_" + str(uuid.uuid4())

    def get_prefetch_count(self) -> int:
        return self._prefetch_count

    def get_receive_endpoint_name(self) -> str:
        return self._receive_endpoint_name

    def get_sqs_config(self) -> Optional[SqsConfig]:
        return self._sqs_config

    def with_prefetch_count(self, prefetch_count: int):
        if prefetch_count < 1:
            raise ValueError("prefetch_count must be >= 1")
        self._prefetch_count = prefetch_count
        return self

    def with_receive_endpoint_name(self, receive_endpoint_name: str):
        if not receive_endpoint_name or receive_endpoint_name == "":
            raise ValueError("receive_endpoint_name must not be empty")
        self._receive_endpoint_name = receive_endpoint_name
        return self

    def with_sqs_config(self, sqs_config: SqsConfig):
        self._sqs_config = sqs_config
        return self

    def build(self):
        if not self._sqs_config:
            raise ValueError("SQS config must be set before building the service bus.")
        from sirabus.servicebus.sqs_servicebus import SqsServiceBus

        return SqsServiceBus(configuration=self)

    @staticmethod
    def default():
        from sirabus.publisher.pydantic_serialization import (
            read_event_message,
            create_command_response,
        )

        return SqsServiceBusConfiguration(
            message_reader=read_event_message,
            command_response_writer=create_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.publisher.cloudevent_serialization import (
            read_cloudevent_message,
            create_command_response,
        )

        return SqsServiceBusConfiguration(
            message_reader=read_cloudevent_message,
            command_response_writer=create_command_response,
        )


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
        headers, event = self._configuration.read(headers, body)
        if isinstance(event, BaseEvent):
            await self.handle_event(event, headers)
        elif isinstance(event, BaseCommand):
            topic_map = self._configuration.get_topic_map()
            command_handler = next(
                (
                    h
                    for h in self._configuration.get_handlers()
                    if (
                        isinstance(h, IHandleCommands)
                        and topic_map.get_from_type(type(event))
                        == topic_map.get_from_type(get_type_param(h))
                    )
                ),
                None,
            )
            if not command_handler:
                if not reply_to:
                    self._configuration.get_logger().error(
                        f"No command handler found for command {type(event)} with correlation ID {correlation_id} "
                        f"and no reply_to field provided."
                    )
                    return
                await self._send_command_response(
                    response=CommandResponse(success=False, message="unknown command"),
                    message_id=message_id,
                    correlation_id=correlation_id,
                    reply_to=reply_to,
                )
                return
            response = await command_handler.handle(command=event, headers=headers)
            if not reply_to:
                self._configuration.get_logger().error(
                    f"Reply to field is empty for command {type(event)} with correlation ID {correlation_id}."
                )
                return
            await self._send_command_response(
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
