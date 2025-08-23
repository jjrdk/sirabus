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


class SqsConfig:
    """
    Configuration class for SQS/SNS clients.
    This class is used to define the configuration for AWS SQS and SNS clients.
    It allows you to specify AWS credentials, region, endpoint URL, and whether to use TLS.
    If a profile name is provided, the access key ID and secret access key are disregarded
    and the profile credentials are used instead.
    :param aws_access_key_id: The AWS access key ID.
    :param aws_secret_access_key: The AWS secret access key.
    :param aws_session_token: The AWS session token (optional).
    :param profile_name: The AWS profile name (optional).
    :param region: The AWS region (default is "us-east-1").
    :param endpoint_url: The endpoint URL for the SQS/SNS service (optional).
    :param use_tls: Whether to use TLS for the connection (default is True).
    :raises ValueError: If the profile name is provided but the access key ID or secret access key is also provided.
    :raises TypeError: If the provided parameters are not of the expected types.
    :raises Exception: If there is an error during client creation or configuration.
    :return: An instance of SqsConfig that can be used to create SQS and SNS clients.
    """

    def __init__(
        self,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        use_tls: bool = True,
    ):
        """
        Defines the configuration for SQS/SNS clients.
        If a profile name is provided, the access key id and secret access are disregarded and the profile credentials
        are used.

        :param aws_access_key_id: The AWS access key id
        :param aws_secret_access_key: The AWS secret access key
        :param aws_session_token: The AWS session token
        :param region: The AWS region
        :param endpoint_url: The endpoint URL
        :param use_tls: Whether to use TLS
        :param profile_name: The profile name
        """
        self._aws_session_token = aws_session_token
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_access_key_id = aws_access_key_id
        self._use_tls = use_tls
        self._region = region
        self._endpoint_url = endpoint_url
        self._profile_name = profile_name

    def to_sns_client(self):
        """
        Creates an SNS client using the provided configuration.
        :return: An SNS client configured with the specified AWS credentials and settings.
        :raises ValueError: If the profile name is provided but the access key ID or secret access key is also provided.
        :raises TypeError: If the provided parameters are not of the expected types.
        :raises Exception: If there is an error during client creation or configuration.
        :rtype: boto3.client
        :raises boto3.exceptions.Boto3Error: If there is an error during client creation
        """
        from boto3 import Session

        session = Session(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
        )
        return session.client(
            service_name="sns",
            region_name=self._region,
            endpoint_url=self._endpoint_url,
            verify=self._use_tls,
        )

    def to_sqs_client(self):
        """
        Creates an SQS client using the provided configuration.
        :return: An SQS client configured with the specified AWS credentials and settings.
        :raises ValueError: If the profile name is provided but the access key ID or secret access key is also provided.
        :raises TypeError: If the provided parameters are not of the expected types.
        :raises Exception: If there is an error during client creation or configuration.
        :rtype: boto3.client
        :raises boto3.exceptions.Boto3Error: If there is an error during client creation
        """
        from boto3 import Session

        session = Session(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
        )
        return session.client(
            service_name="sqs",
            region_name=self._region,
            endpoint_url=self._endpoint_url,
            verify=self._use_tls,
        )
