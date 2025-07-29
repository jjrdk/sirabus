import asyncio
from typing import Callable, List, Tuple

from aett.eventstore import BaseEvent

from sirabus import IHandleEvents
from sirabus.message_pump import MessageConsumer, MessagePump
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus


class InMemoryServiceBus(ServiceBus, MessageConsumer):
    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]
        ],
        handlers: List[IHandleEvents],
        message_pump: MessagePump,
    ) -> None:
        super().__init__(
            topic_map=topic_map, message_reader=message_reader, handlers=handlers
        )
        self._message_pump = message_pump
        self._subscription = None

    async def handle_message(self, headers: dict, body: bytes) -> None:
        await self.handle_event(headers, body)

    async def run(self):
        if not self._subscription:
            self._subscription = self._message_pump.register_consumer(self)
        await asyncio.sleep(0)

    async def stop(self):
        if self._subscription:
            self._message_pump.unregister_consumer(self._subscription)
        await asyncio.sleep(0)
