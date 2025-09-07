from typing import Any, Dict, Optional

import google.auth.credentials
from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.client_info import ClientInfo
from google.pubsub_v1 import (
    PublisherAsyncClient,
    SubscriberAsyncClient,
)


class ServiceAccountInfo(Dict[str, Any]):
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str
    universe_domain: Optional[str] = "googleapis.com"
    type: Optional[str] = "service_account"


class PubSubConfig:
    """
    Configuration class for Pub/Sub clients.
    This class is used to define the configuration for Google Cloud Pub/Sub clients.
    It allows you to specify the project ID and the path to the service account JSON file.
    """

    def __init__(
        self,
        project_id: str,
        creds: google.auth.credentials.Credentials,
        options: Optional[ClientOptions] = None,
        custom_ca_cert: Optional[str] = None,
    ):
        """
        Defines the configuration for Pub/Sub clients.

        :param project_id: The Google Cloud project ID
        """
        self._project_id = project_id
        self.__credentials = creds
        self.__options = options
        self.__custom_ca_cert = custom_ca_cert

    def get_project_id(self) -> str:
        """
        Gets the project ID for the Pub/Sub configuration.
        :return: The Google Cloud project ID.
        """
        return self._project_id

    def to_publisher_client(self) -> PublisherAsyncClient:
        """
        Creates a Pub/Sub publisher client using the provided configuration.
        :return: A Pub/Sub publisher client configured with the specified project ID and service account JSON file.
        :raises Exception: If there is an error during client creation or configuration.
        :rtype: google.cloud.pubsub_v1.PublisherClient
        :raises google.api_core.exceptions.GoogleAPIError: If there is an error during client creation
        """

        return PublisherAsyncClient(
            credentials=self.__credentials,
            client_options=self.__options,
            client_info=ClientInfo(user_agent=f"sira-bus/{self._project_id}"),
        )

    def to_subscriber_client(self) -> SubscriberAsyncClient:
        """
        Creates a Pub/Sub subscriber client using the provided configuration.
        :return: A Pub/Sub subscriber client configured with the specified project ID and service account JSON file.
        :raises Exception: If there is an error during client creation or configuration.
        :rtype: google.cloud.pubsub_v1.SubscriberClient
        :raises google.api_core.exceptions.GoogleAPIError: If there is an error during client creation
        """

        return SubscriberAsyncClient(
            credentials=self.__credentials,
            client_options=self.__options,
            client_info=ClientInfo(user_agent=f"sira-bus/{self._project_id}"),
        )
