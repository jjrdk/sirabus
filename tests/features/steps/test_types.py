import asyncio
import threading

from aett.eventstore import BaseEvent, Topic
from aett.eventstore.base_command import BaseCommand

from sirabus import IHandleEvents


@Topic("test")
class TestEvent(BaseEvent):
    pass


@Topic("test_sub")
class SubTestEvent(TestEvent):
    pass


@Topic("nested")
class NestedTestEvent(SubTestEvent):
    pass


@Topic("other")
class OtherTestEvent(BaseEvent):
    pass


@Topic("get_status")
class StatusCommand(BaseCommand):
    pass


@Topic("get_info")
class InfoCommand(BaseCommand):
    pass


@Topic("invalid_command")
class InvalidCommand(BaseCommand):
    pass


class TestEventHandler(IHandleEvents[TestEvent]):
    def __init__(self, wait_handle: asyncio.Event):
        self.wait_handle = wait_handle
        super().__init__()

    async def handle[TEvent: BaseEvent](self, event: TEvent, headers: dict) -> None:
        self.wait_handle.set()
        await asyncio.sleep(0)


class OtherTestEventHandler(IHandleEvents[OtherTestEvent]):
    def __init__(self, wait_handle: threading.Event):
        self.wait_handle = wait_handle
        super().__init__()

    async def handle(self, event: OtherTestEvent, headers: dict) -> None:
        self.wait_handle.set()
        await asyncio.sleep(0)
