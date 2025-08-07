import asyncio
from abc import ABC, abstractmethod

from aett.eventstore import Topic, BaseCommand, BaseEvent
from pydantic import BaseModel, Field


@Topic("command_response")
class CommandResponse(BaseModel):
    """
    Represents a response to a command.
    This class can be extended to provide specific response types.
    """

    success: bool = Field(
        default=True, description="Indicates if the command was successful"
    )
    message: str = Field(
        default="",
        description="A message providing additional information about the command response",
    )

    def __repr__(self) -> str:
        return f"CommandResponse(success={self.success}, message='{self.message}')"


class IRouteCommands(ABC):
    """
    Interface for routing commands. The command router expects to receive replies to commands
    """

    @abstractmethod
    async def route[TCommand:BaseCommand](self, command: TCommand) -> asyncio.Future[CommandResponse]:
        """
        Route a command.

        :param command: The command to route.
        :return: A CommandResponse indicating the success or failure of the command routing.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


class IHandleCommands[TCommand:BaseCommand](ABC):
    """
    Interface for handling commands.
    """

    @abstractmethod
    async def handle(self, command: TCommand, headers: dict) -> CommandResponse:
        """
        Handle a command.

        :param command: The command to handle.
        :param headers: Additional headers associated with the command.
        :return: A CommandResponse indicating the success or failure of the command handling.
        """

        raise NotImplementedError("This method should be overridden by subclasses.")


class IPublishEvents(ABC):
    """
    Interface for publishing events.
    """

    @abstractmethod
    async def publish[TEvent:BaseEvent](self, event: TEvent) -> None:
        """
        Publish an event.

        :param event: The event to publish.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


class IHandleEvents[TEvent:BaseEvent](ABC):
    """
    Interface for handling events.
    """

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


def get_type_param(instance: IHandleCommands | IHandleEvents) -> type:
    from typing import get_args
    t = type(instance)
    orig_bases__ = t.__orig_bases__
    return get_args(orig_bases__[0])[0]
