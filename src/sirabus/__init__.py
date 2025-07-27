import abc
import asyncio
import threading
import time
from abc import ABC, abstractmethod
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import Generic, Type, TypeVar, get_args, Optional, Dict, Tuple, Coroutine
from uuid import UUID, uuid4

from aett.eventstore.base_command import BaseCommand
from pydantic import BaseModel, Field

from aett.eventstore import BaseEvent, Topic

TEvent = TypeVar("TEvent", bound=BaseEvent, contravariant=True)
TCommand = TypeVar("TCommand", bound=BaseCommand, contravariant=True)


@Topic("command_response")
class CommandResponse(BaseModel):
    """
    Represents a response to a command.
    This class can be extended to provide specific response types.
    """

    success: bool = Field(
        default=True, description="Indicates if the command was successful"
    )
    message: Optional[str] = Field(
        default="",
        description="A message providing additional information about the command response",
    )

    def __repr__(self) -> str:
        return f"CommandResponse(success={self.success}, message='{self.message}')"


class IRouteCommands(ABC, Generic[TCommand]):
    """
    Interface for routing commands. The command router expects to receive replies to commands
    """

    @abstractmethod
    async def route(self, command: TCommand) -> CommandResponse:
        """
        Route a command.

        :param command: The command to route.
        :return: A CommandResponse indicating the success or failure of the command routing.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


class IPublishEvents(ABC, Generic[TEvent]):
    """
    Interface for publishing events.
    """

    @abstractmethod
    async def publish(self, event: TEvent) -> None:
        """
        Publish an event.

        :param event: The event to publish.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


class IHandleEvents(ABC, Generic[TEvent]):
    """
    Interface for handling events.
    """

    event_type: Type[TEvent]

    def __init_subclass__(cls, **kwargs):
        cls.event_type = get_args(cls.__orig_bases__[0])[0]

    @abstractmethod
    async def handle(self, event: TEvent, headers: dict) -> None:
        """
        Handle an event.

        :param event: The event to handle.
        :param headers: Additional headers associated with the event.
        :return: None
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


def generate_vhost_name(name: str, version: str) -> str:
    """
    Generates a virtual host name based on the application name and version.
    :param name: The name of the application.
    :param version: The version of the application.
    :return: A string representing the virtual host name.
    """
    import hashlib

    h = hashlib.sha256(usedforsecurity=False)
    h.update(f"{name}_{version}".encode())
    return h.hexdigest()


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
                    asyncio.gather(*[consumer.handle_message(headers, body) for consumer in self._consumers.values()])
                )
                print(f"Processed message with headers: {headers} and body: {body}")
            else:
                time.sleep(0.1)

    def stop(self):
        """
        Stop the message pump.
        """
        self._stopped = True
