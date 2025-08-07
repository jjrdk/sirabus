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
        self._event_writer = event_writer
        self.__topic_map = topic_map
        self.__messagepump = messagepump
        self.__logger = logger or logging.getLogger("InMemoryPublisher")

    async def publish[TEvent:BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic in memory.
        :param event: The event to publish.
        """

        topic, hierarchical_topic, j = self._event_writer(event, self.__topic_map)
        self.__messagepump.publish(({"topic": topic}, j.encode()))
        await asyncio.sleep(0)
