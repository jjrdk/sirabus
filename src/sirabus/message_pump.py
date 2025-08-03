import abc
import asyncio
import logging
import threading
import time
from queue import Queue
from typing import Dict, Tuple
from uuid import UUID, uuid4


class MessageConsumer(abc.ABC):
    def __init__(self):
        self.id = uuid4()

    @abc.abstractmethod
    async def handle_message(
        self,
        headers: dict,
        body: bytes,
        correlation_id: str | None,
        reply_to: str | None,
    ) -> None:
        """
        Handle a message with the given headers and body.
        :param headers: The message headers.
        :param body: The message body.
        :param correlation_id: The correlation ID of the message.
        :param reply_to: The reply-to address for the message.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class MessagePump:
    def __init__(self, logger: logging.Logger | None = None):
        self._consumers: Dict[UUID, MessageConsumer] = dict()
        self._messages: Queue[Tuple[dict, bytes]] = Queue()
        self._task = None
        self._stopped = False
        self._logger = logger or logging.getLogger("MessagePump")

    def register_consumer(self, consumer: MessageConsumer) -> UUID:
        """
        Register a new consumer.
        :param consumer: The consumer to register.
        :return: A unique identifier for the consumer.
        """
        self._consumers[consumer.id] = consumer
        return consumer.id

    def unregister_consumer(self, consumer_id: UUID):
        """
        Unregister a consumer.
        :param consumer_id: The unique identifier of the consumer to unregister.
        """
        if consumer_id in self._consumers:
            del self._consumers[consumer_id]

    def publish(self, message: Tuple[dict, bytes]):
        self._messages.put(message)

    def start(self):
        if self._task:
            return
        self._task = threading.Thread(target=self._consume, daemon=True)
        self._task.start()

    def _consume(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while not self._stopped:
            if not self._messages.empty():
                headers, body = self._messages.get()
                results = loop.run_until_complete(
                    asyncio.gather(
                        *[
                            consumer.handle_message(
                                headers,
                                body,
                                correlation_id=headers.get("correlation_id", None),
                                reply_to=headers.get("reply_to", None),
                            )
                            for consumer in self._consumers.values()
                        ]
                    )
                )
                if headers.get("reply_to", None) is not None:
                    self._logger.debug(f"Reply to {headers.get('reply_to')}")
                    try:
                        message = next(r for r in results if r is not None)
                        self.publish(message)
                    except StopIteration:
                        self._logger.debug("Nothing to reply with.")

                self._logger.debug(
                    f"Processed message with headers: {headers} and body: {body}"
                )
            else:
                time.sleep(0.1)

    def stop(self):
        """
        Stop the message pump.
        """
        self._stopped = True
        if self._task:
            self._task.join(timeout=5)
            self._task = None
