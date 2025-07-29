import abc
from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar, get_args, Optional

from aett.eventstore import BaseEvent, Topic
from aett.eventstore.base_command import BaseCommand
from pydantic import BaseModel, Field

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
