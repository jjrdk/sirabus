import asyncio
from typing import Callable, Tuple, Optional, Self

from aett.eventstore import BaseEvent, BaseCommand

from sirabus import CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessageConsumer, MessagePump
from sirabus.servicebus import ServiceBus, ServiceBusConfiguration


class InMemoryConfiguration(ServiceBusConfiguration):
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
        self._message_pump: Optional[MessagePump] = None
        self.response_writer = command_response_writer

    def get_message_pump(self) -> MessagePump:
        """
        Get the message pump.
        :return: The message pump.
        :rtype: MessagePump
        :raises ValueError: If the message pump is not set.
        """
        if not self._message_pump:
            raise ValueError("Message pump is not set.")
        return self._message_pump

    def with_message_pump(self, message_pump: MessagePump) -> Self:
        """
        Set the message pump.
        :param message_pump: The message pump.
        :return: The in-memory service bus configuration.
        :rtype: InMemoryConfiguration
        :raises ValueError: If the message pump is None.
        """
        self._message_pump = message_pump
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            read_event,
            write_command_response,
        )

        return InMemoryConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            read_event,
            write_command_response,
        )

        return InMemoryConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_custom(message_reader, command_response_writer):
        return InMemoryConfiguration(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )


class InMemoryServiceBus(ServiceBus[InMemoryConfiguration], MessageConsumer):
    def __init__(self, configuration: InMemoryConfiguration) -> None:
        """
        Initializes the InMemoryServiceBus.
        :param configuration: The in-memory service bus configuration.
        :raises ValueError: If the message reader cannot determine the topic for the event or command.
        :raises TypeError: If the event or command is not a subclass of BaseEvent or BaseCommand.
        :raises Exception: If there is an error during message handling or response sending.
        :return: None
        """
        ServiceBus.__init__(self, configuration)
        MessageConsumer.__init__(self)
        self._subscription = None

    async def run(self):
        if not self._subscription:
            pump = self._configuration.get_message_pump()
            self._subscription = pump.register_consumer(self)
        await asyncio.sleep(0)

    async def stop(self):
        if self._subscription:
            self._configuration.get_message_pump().unregister_consumer(
                self._subscription
            )
        await asyncio.sleep(0)

    async def handle_message(
        self,
        headers: dict,
        body: bytes,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str | None,
    ) -> None:
        await self._handle_message(headers, body, message_id, correlation_id, reply_to)

    async def _send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ) -> None:
        topic, message = self._configuration.write_response(response)
        headers = {"topic": topic, "reply_to": reply_to}
        if correlation_id:
            headers["correlation_id"] = correlation_id
        if message_id:
            headers["message_id"] = message_id
        self._configuration.get_message_pump().publish((headers, message))
