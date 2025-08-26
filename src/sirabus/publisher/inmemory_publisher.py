from typing import Callable, Tuple, Self

from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump
from sirabus.publisher import PublisherConfiguration


class InMemoryPublisherConfiguration(PublisherConfiguration):
    def __init__(
        self, event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str]]
    ):
        """
        Initializes the In-Memory Publisher Configuration with the necessary parameters.
        :param event_writer: A callable that formats the event into a message.
        """
        super().__init__(event_writer=event_writer)
        self._message_pump: MessagePump | None = None

    def get_message_pump(self) -> MessagePump:
        if not self._message_pump:
            raise ValueError("message_pump has not been set.")
        return self._message_pump

    def with_message_pump(self, message_pump: MessagePump) -> Self:
        if not message_pump:
            raise ValueError("message_pump cannot be None.")
        self._message_pump = message_pump
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import write_event

        return InMemoryPublisherConfiguration(event_writer=write_event)

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import write_event

        return InMemoryPublisherConfiguration(event_writer=write_event)


class InMemoryPublisher(IPublishEvents):
    """
    Publishes events in memory.
    """

    def __init__(
        self,
        configuration: InMemoryPublisherConfiguration,
    ) -> None:
        """
        Initializes the InMemoryPublisher with a topic map, message pump, and event writer.
        :param configuration: The publisher configuration.
        """
        self._configuration = configuration

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic in memory.
        :param event: The event to publish.
        """

        hierarchical_topic, j = self._configuration.write_event(event)
        self._configuration.get_message_pump().publish(
            ({"topic": hierarchical_topic}, j.encode())
        )
