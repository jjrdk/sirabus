import abc
import asyncio
from typing import Tuple, Callable, List

from aett.eventstore import BaseEvent
from aett.eventstore.base_command import BaseCommand

from sirabus import IHandleEvents, IHandleCommands, CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class ServiceBus(abc.ABC):
    def __init__(
            self,
            topic_map: HierarchicalTopicMap,
            message_reader: Callable[
                [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
            ],
            handlers: List[IHandleEvents | IHandleCommands],
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

    async def handle_message(self, headers: dict, body: bytes) -> CommandResponse | None:
        headers, event = self._message_reader(self._topic_map, headers, body)
        if isinstance(event, BaseEvent):
            await self.handle_event(event, headers)
            return None
        elif isinstance(event, BaseCommand):
            command_handler = next(
                (h for h in self._handlers if
                 isinstance(h, IHandleCommands) and isinstance(event, type(h.message_type))),
                None
            )
            if not command_handler:
                raise RuntimeError("No command handler found for command type: " + str(type(event)))
            return await command_handler.handle(command=event, headers=headers)
        else:
            raise TypeError(f"Unexpected message type: {type(event)}")

    async def handle_event(self, event: BaseEvent, headers: dict) -> None:
        await asyncio.gather(
            *[
                h.handle(event=event, headers=headers)
                for h in self._handlers
                if isinstance(h, IHandleEvents) and isinstance(event, type(h).message_type)
            ],
            return_exceptions=True,
        )
