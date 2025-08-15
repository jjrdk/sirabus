import asyncio
import logging
from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.message_pump import MessagePump


class InMemoryPublisher(IPublishEvents):
    """
    Publishes events in memory.
    """

    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        messagepump: MessagePump,
        event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str, str]],
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initializes the InMemoryPublisher with a topic map, message pump, and event writer.
        :param topic_map: The hierarchical topic map to use for event topics.
        :param messagepump: The message pump to use for publishing messages.
        :param event_writer: A callable that takes an event and topic map and returns a tuple
                            containing the topic, hierarchical topic, and JSON representation of the event.
        :param logger: An optional logger for logging events.
        """
        self._event_writer = event_writer
        self.__topic_map = topic_map
        self.__messagepump = messagepump
        self.__logger = logger or logging.getLogger("InMemoryPublisher")

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic in memory.
        :param event: The event to publish.
        """

        _, hierarchical_topic, j = self._event_writer(event, self.__topic_map)
        self.__messagepump.publish(({"topic": hierarchical_topic}, j.encode()))
        await asyncio.sleep(0)
