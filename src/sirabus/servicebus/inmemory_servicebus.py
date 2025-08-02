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
                [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent|BaseCommand]
            ],
            handlers: List[IHandleEvents | IHandleCommands],
            message_pump: MessagePump,
            logger: logging.Logger,
    ) -> None:
        super().__init__(
            topic_map=topic_map, message_reader=message_reader, handlers=handlers, logger=logger
        )
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

    async def send_command_response(self, response: CommandResponse, correlation_id: str | None, reply_to: str) -> None:
        pass
