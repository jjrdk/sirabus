import abc
import asyncio
import threading
import time
from queue import Queue
from typing import Dict, Tuple
from uuid import UUID, uuid4


class MessageConsumer(abc.ABC):
    @abc.abstractmethod
    async def handle_message(self, headers: dict, body: bytes) -> None:
        """
        Handle a message with the given headers and body.
        :param headers: The message headers.
        :param body: The message body.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class MessagePump:
    def __init__(self):
        self._consumers: Dict[UUID, MessageConsumer] = dict()
        self._messages: Queue[Tuple[dict, bytes]] = Queue()
        self._task = None
        self._stopped = False

    def register_consumer(self, consumer: MessageConsumer) -> UUID:
        """
        Register a new consumer.
        :param consumer: The consumer to register.
        :return: A unique identifier for the consumer.
        """
        consumer_id = uuid4()
        self._consumers[consumer_id] = consumer
        return consumer_id

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
                loop.run_until_complete(
                    asyncio.gather(
                        *[
                            consumer.handle_message(headers, body)
                            for consumer in self._consumers.values()
                        ]
                    )
                )
                logging.info(f"Processed message with headers: {headers} and body: {body}")
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
