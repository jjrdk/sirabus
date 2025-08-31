import abc
from typing import Tuple, Callable

from aett.eventstore.base_command import BaseCommand

from sirabus import CommandResponse, EndpointConfiguration
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class RouterConfiguration(EndpointConfiguration, abc.ABC):
    """Abstract Configuration for Command Router."""

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
        super().__init__()
        self._message_writer: Callable[
            [BaseCommand, HierarchicalTopicMap], Tuple[str, str]
        ] = message_writer
        self._response_reader: Callable[[dict, bytes], CommandResponse | None] = (
            response_reader
        )

    def write_message(self, command: BaseCommand) -> Tuple[str, str]:
        return self._message_writer(command, self.get_topic_map())

    def read_response(self, headers: dict, body: bytes) -> CommandResponse | None:
        return self._response_reader(headers, body)
