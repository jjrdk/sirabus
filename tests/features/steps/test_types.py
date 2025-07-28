import asyncio
import threading

from aett.eventstore import BaseEvent, Topic

from sirabus import TEvent, IHandleEvents


@Topic("test")
class TestEvent(BaseEvent):
    pass


@Topic("test_sub")
class SubTestEvent(TestEvent):
    pass

@Topic("other")
class OtherTestEvent(BaseEvent):
    pass


class TestEventHandler(IHandleEvents[TestEvent]):
    def __init__(self, wait_handle: threading.Event):
        self.wait_handle = wait_handle
        super().__init__()

    async def handle(self, event: TEvent, headers: dict) -> None:
        self.wait_handle.set()
        await asyncio.sleep(0.01)
