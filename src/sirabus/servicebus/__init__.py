import abc
import asyncio
from typing import Tuple, Callable, List

from aett.eventstore import BaseEvent

from sirabus import IHandleEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class ServiceBus(abc.ABC):
    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]
        ],
        handlers: List[IHandleEvents],
    ) -> None:
        self._topic_map = topic_map
        self._message_reader = message_reader
        self._handlers = handlers

    @abc.abstractmethod
    async def run(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def stop(self):
        raise NotImplementedError()

    async def handle_event(self, headers: dict, body: bytes) -> None:
        headers, event = self._message_reader(self._topic_map, headers, body)
        if not isinstance(event, BaseEvent):
            raise TypeError(f"Expected event of type BaseEvent, got {type(event)}")
        await asyncio.gather(
            *[
                h.handle(event=event, headers=headers)
                for h in self._handlers
                if hasattr(type(h), "event_type") and isinstance(event, type(h).event_type)
            ],
            return_exceptions=True,
        )
