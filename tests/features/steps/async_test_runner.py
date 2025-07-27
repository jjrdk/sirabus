import asyncio


class AsyncTestRunner:
    """Helper to run async operations in Behave steps"""

    def __init__(self):
        self.loop = None

    def run_async(self, coro):
        """Run coroutine in event loop"""
        if self.loop is None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        return self.loop.run_until_complete(coro)

    def close(self):
        """Close the event loop and executor"""
        if self.loop:
            self.loop.close()
