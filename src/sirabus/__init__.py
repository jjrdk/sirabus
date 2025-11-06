import asyncio
import logging
from abc import ABC, abstractmethod
from ssl import SSLContext
from typing import Optional, Self

from aett.eventstore import BaseCommand, BaseEvent

from sirabus.command_response import CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class IRouteCommands(ABC):
    """
    Interface for routing commands. The command router expects to receive replies to commands
    """

    from sirabus.command_response import CommandResponse

    @abstractmethod
    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        """
        Route a command.

        :param command: The command to route.
        :return: A CommandResponse indicating the success or failure of the command routing.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


class IHandleCommands[TCommand: BaseCommand](ABC):
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
    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publish an event.

        :param event: The event to publish.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


class IHandleEvents[TEvent: BaseEvent](ABC):
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


def get_type_param(instance: IHandleCommands | IHandleEvents) -> type:
    """
    Extracts the type parameter from an instance of IHandleCommands or IHandleEvents.
    This function uses the `get_args` function from the `typing` module to retrieve the
    type parameter from the generic type of the instance.
    :param instance: An instance of IHandleCommands or IHandleEvents.
    :return: The type parameter of the instance.
    """
    from typing import get_args

    t = type(instance)
    orig_bases__ = t.__orig_bases__
    return get_args(orig_bases__[0])[0]


class EndpointConfiguration(ABC):
    def __init__(self):
        """
        Initializes the EndpointConfiguration with default values.
        The default topic map is an instance of HierarchicalTopicMap. The default logger is set to "ServiceBus".
        The SSL/TLS configuration and CA certificate file are initialized to None.
        """
        self._topic_map = HierarchicalTopicMap()
        self._logger = logging.getLogger("ServiceBus")
        self._ssl_config = None
        self._ca_cert_file = None
        self._timeout_seconds = 30

    def get_topic_map(self) -> HierarchicalTopicMap:
        """
        Gets the topic map for the endpoint configuration.
        :return: An instance of HierarchicalTopicMap used for topic mapping.
        """
        return self._topic_map

    def get_logger(self) -> logging.Logger:
        """
        Gets the logger for the endpoint configuration.
        :return: An instance of logging.Logger used for logging.
        """
        return self._logger

    def get_timeout_seconds(self) -> int:
        """
        Gets the timeout in seconds for the endpoint configuration.
        :return: The timeout in seconds.
        """
        return self._timeout_seconds

    def get_ssl_config(self) -> Optional[SSLContext]:
        """
        Gets the SSL/TLS configuration for secure connections.
        """
        return self._ssl_config

    def get_ca_cert_file(self) -> Optional[str]:
        """
        Gets the CA certificate file path for SSL/TLS connections.
        """
        return self._ca_cert_file

    def with_timeout_seconds(self, timeout_seconds: int) -> Self:
        """
        Sets the timeout in seconds for the endpoint configuration.
        :param timeout_seconds: The timeout in seconds.
        :return: The EndpointConfiguration instance.
        """
        self._timeout_seconds = timeout_seconds
        return self

    def with_topic_map(self, topic_map: HierarchicalTopicMap) -> Self:
        """
        Sets the topic map for the endpoint configuration.
        :param topic_map: An instance of HierarchicalTopicMap to be used for topic mapping.
        :return: The EndpointConfiguration instance.
        """
        self._topic_map = topic_map
        return self

    def with_logger(self, logger: logging.Logger) -> Self:
        """
        Sets the logger for the endpoint configuration.
        :param logger: An instance of logging.Logger to be used for logging.
        :return: The EndpointConfiguration instance.
        """
        self._logger = logger
        return self

    def with_ssl_config(self, ssl_config: SSLContext):
        """
        Sets the SSL/TLS configuration for secure connections.
        :param ssl_config: An instance of ssl.SSLContext containing the SSL/TLS configuration.
        :return: The EndpointConfiguration instance.
        :raises ValueError: If the ssl_config is not an instance of ssl.SSLContext
        """
        if not isinstance(ssl_config, SSLContext):
            raise ValueError("ssl_config must be an instance of ssl.SSLContext")
        self._ssl_config = ssl_config
        return self

    def with_ca_cert_file(self, ca_cert_file: str) -> Self:
        """
        Sets the CA certificate file path for SSL/TLS connections.
        This is generally expected to be used in conjunction with a custom SSLContext.
        The CA certificate file is generally a PEM file containing one or more CA certificates, but can be in other
        formats depending on the SSL library being used.
        :param ca_cert_file: The file path to the CA certificate.
        :return: The EndpointConfiguration instance.
        :raises ValueError: If the ca_cert_file is not a valid file path.
        """
        import os

        if not os.path.isfile(ca_cert_file):
            raise ValueError("ca_cert_file must be a valid file path")
        self._ca_cert_file = ca_cert_file
        return self

    @staticmethod
    @abstractmethod
    def default():
        """
        Creates a default EndpointConfiguration instance with standard serialization methods.
        :return: An EndpointConfiguration instance with default settings.
        """
        ...

    @staticmethod
    @abstractmethod
    def for_cloud_event():
        """
        Creates an EndpointConfiguration instance configured for CloudEvents serialization.
        :return: An EndpointConfiguration instance with CloudEvents settings.
        """
        ...
