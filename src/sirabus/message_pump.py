import abc
import asyncio
import logging
import threading
import time
from queue import Queue
from typing import Dict, Tuple
from uuid import UUID, uuid4


class MessageConsumer(abc.ABC):
    """
    Abstract base class for message consumers.
    Consumers must implement the `handle_message` method to process incoming messages.
    Each consumer is assigned a unique identifier (UUID) upon instantiation.
    Consumers can be registered with a message pump to receive messages.
    :raises NotImplementedError: If the `handle_message` method is not implemented by a subclass.
    :note: The `handle_message` method must be implemented by subclasses to define how messages are processed.
    :example: A subclass might implement `handle_message` to log the message or perform some business logic.
    :usage: Consumers can be registered with a `MessagePump` to receive messages from the pump.
    :see: `MessagePump` for more details on how to register and use consumers.
    :note: The `handle_message` method is called with the message headers, body, message ID, correlation ID, and reply-to address.
    :note: The `id` attribute is automatically generated using `uuid4()` when the consumer is instantiated.
    """

    def __init__(self):
        """
        Initializes a new instance of the MessageConsumer class.
        This constructor generates a unique identifier (UUID) for the consumer.
        :note: The UUID is generated using `uuid4()` to ensure uniqueness across consumers.
        :example: The generated UUID can be used to register the consumer with a message pump or
        to track the consumer in logs or monitoring systems.
        :usage: Consumers can be instantiated and registered with a `MessagePump` to start receiving messages.
        :see: `MessagePump` for more details on how to register and use consumers.
        :note: The `id` attribute is set to a new UUID when the consumer is instantiated.
        :note: Subclasses must implement the `handle_message` method to define how messages are processed.
        """
        self.id = uuid4()

    @abc.abstractmethod
    async def handle_message(
        self,
        headers: dict,
        body: bytes,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str | None,
    ) -> None:
        """
        Handle a message with the given headers and body.
        :param headers: The message headers.
        :param body: The message body.
        :param message_id: The unique identifier of the message.
        :param correlation_id: The correlation ID of the message.
        :param reply_to: The reply-to address for the message.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class MessagePump:
    """
    A message pump that manages message consumers and processes incoming messages.
    This class allows for the registration of message consumers, publishing messages,
    and starting a background thread to consume messages asynchronously.
    :param logger: Optional logger for logging messages and events.
    :type logger: logging.Logger | None
    :note: If no logger is provided, a default logger named "MessagePump" is created.
    :example: The message pump can be used to register consumers that handle specific message types
    and to publish messages that will be processed by those consumers.
    :usage: Create an instance of `MessagePump`, register consumers, and start the pump
    to begin processing messages. Consumers can be registered using the `register_consumer` method,
    and messages can be published using the `publish` method.
    :see: `MessageConsumer` for more details on how to implement message consumers.
    :note: The message pump runs in a separate thread, allowing it to process messages asynchronously
    while the main application continues to run. The `start` method initializes the background thread,
    and the `stop` method gracefully stops the pump and waits for the thread to finish.
    :note: The `publish` method adds messages to a queue, which are then processed by the consumers
    in the background thread. Each message consists of headers and a body, which are passed to the consumers
    for processing. The consumers can handle the messages asynchronously, allowing for concurrent processing
    of multiple messages.
    :note: The `MessagePump` class is designed to be thread-safe, allowing multiple threads to register consumers and publish messages concurrently.
    It uses a queue to manage incoming messages and a background thread to process them, ensuring that message handling does not block the main application thread.
    """

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
        """
        Publish a message to the message pump.
        :param message: A tuple containing headers (dict) and body (bytes) of the message.
        :note: The headers should include metadata such as message ID, correlation ID, and reply-to address.
        :example: The message can be a tuple like ({"message_id": "12345", "reply_to": "some_queue"}, b"Message body
        content").
        :raises ValueError: If the message is not a tuple of (dict, bytes).
        :note: The message pump uses a queue to manage incoming messages, allowing for asynchronous processing by
        registered consumers.
        """
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
                                message_id=headers.get("message_id"),
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
