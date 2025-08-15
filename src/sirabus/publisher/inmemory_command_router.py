import asyncio
import logging
from typing import List, Tuple, Callable, Optional

from aett.eventstore.base_command import BaseCommand

from sirabus import IRouteCommands, CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump, MessageConsumer


class InMemoryCommandRouter(IRouteCommands):
    """In-memory Command Router for handling commands and responses.
    This class implements the IRouteCommands interface to route commands
    using an in-memory message pump. It manages the registration of consumers
    for handling command responses and publishes commands to the message pump.
    """

    def __init__(
        self,
        message_pump: MessagePump,
        topic_map: HierarchicalTopicMap,
        command_writer: Callable[
            [BaseCommand, HierarchicalTopicMap], Tuple[str, str, str]
        ],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initializes the InMemoryCommandRouter with the necessary parameters.
        :param message_pump: The message pump for handling messages.
        :param topic_map: The hierarchical topic map for routing commands.
        :param command_writer: A callable that formats the command into a message.
        :param response_reader: A callable that reads the response from the message.
        :param logger: Optional logger for logging debug information.
        :raises ValueError: If the message_pump is None or if the topic_map is None.
        :raises TypeError: If command_writer or response_reader are not callable.
        :raises TypeError: If command is not a subclass of BaseCommand.
        :raises ValueError: If the command cannot be serialized.
        """
        self._response_reader = response_reader
        self._command_writer = command_writer
        self._message_pump = message_pump
        self._topic_map = topic_map
        self._logger = logger or logging.getLogger("InMemoryCommandRouter")
        self._consumers: List[MessageConsumer] = []

    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        response_future = asyncio.get_event_loop().create_future()
        try:
            headers, message = self._create_message(command)
        except ValueError:
            response_future.set_result(
                CommandResponse(success=False, message="unknown command")
            )
            return response_future
        consumer = ResponseConsumer(
            parent_cleanup=self._remove_consumer,
            message_pump=self._message_pump,
            future=response_future,
            response_reader=self._response_reader,
        )
        self._message_pump.register_consumer(consumer)
        self._consumers.append(consumer)
        headers["topic"] = self._topic_map.get_from_type(type(command))
        headers["reply_to"] = consumer.id
        self._message_pump.publish((headers, message))
        return response_future

    def _create_message[TCommand: BaseCommand](
        self,
        command: TCommand,  # type: ignore
    ) -> Tuple[dict, bytes]:
        _, __, j = self._command_writer(command, self._topic_map)
        return {}, j.encode()

    def _remove_consumer(self, consumer: MessageConsumer) -> None:
        self._consumers.remove(consumer)


class ResponseConsumer(MessageConsumer):
    """ResponseConsumer for handling command responses. This class extends MessageConsumer to handle incoming messages
    that are responses to commands. It reads the response and sets the result on the associated future.
    """

    def __init__(
        self,
        parent_cleanup: Callable[[MessageConsumer], None],
        message_pump: MessagePump,
        future: asyncio.Future[CommandResponse],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
    ) -> None:
        super().__init__()
        self._response_reader = response_reader
        self._parent_cleanup = parent_cleanup
        self._future: asyncio.Future[CommandResponse] = future
        self._message_pump = message_pump

    async def handle_message(
        self,
        headers: dict,
        body: bytes,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str | None,
    ) -> None:
        if reply_to == self.id:
            response = self._response_reader(headers, body)
            if not response:
                return
            self._message_pump.unregister_consumer(self.id)
            self._parent_cleanup(self)
            self._future.set_result(response)
