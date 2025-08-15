import asyncio
import logging
from typing import Callable, List, Tuple

from aett.eventstore import BaseEvent
from aett.eventstore.base_command import BaseCommand

from sirabus import IHandleEvents, CommandResponse, IHandleCommands
from sirabus.message_pump import MessageConsumer, MessagePump
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus


class InMemoryServiceBus(ServiceBus, MessageConsumer):
    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ],
        response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
        handlers: List[IHandleEvents | IHandleCommands],
        message_pump: MessagePump,
        logger: logging.Logger,
    ) -> None:
        """
        Initializes the InMemoryServiceBus.
        :param topic_map: The hierarchical topic map for topic resolution.
        :param message_reader: A callable that reads the message and returns headers and an event or command.
        :param response_writer: A callable that formats the command response for sending.
        :param handlers: A list of event and command handlers.
        :param message_pump: The message pump for handling message consumption and publishing.
        :param logger: Optional logger for logging.
        :raises ValueError: If the message reader cannot determine the topic for the event or command.
        :raises TypeError: If the event or command is not a subclass of BaseEvent or BaseCommand.
        :raises Exception: If there is an error during message handling or response sending.
        :return: None
        """
        ServiceBus.__init__(
            self,
            topic_map=topic_map,
            message_reader=message_reader,
            handlers=handlers,
            logger=logger,
        )
        MessageConsumer.__init__(self)
        self._response_writer = response_writer
        self._message_pump = message_pump
        self._subscription = None

    async def run(self):
        if not self._subscription:
            self._subscription = self._message_pump.register_consumer(self)
        await asyncio.sleep(0)

    async def stop(self):
        if self._subscription:
            self._message_pump.unregister_consumer(self._subscription)
        await asyncio.sleep(0)

    async def send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ) -> None:
        topic, message = self._response_writer(response)
        headers = {"topic": topic, "reply_to": reply_to}
        if correlation_id:
            headers["correlation_id"] = correlation_id
        if message_id:
            headers["message_id"] = message_id
        self._message_pump.publish((headers, message))
