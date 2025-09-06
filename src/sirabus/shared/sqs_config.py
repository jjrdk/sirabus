class SqsConfig:
    """
    Configuration class for SQS/SNS clients.
    This class is used to define the configuration for AWS SQS and SNS clients.
    It allows you to specify AWS credentials, region, endpoint URL, and whether to use TLS.
    If a profile name is provided, the access key ID and secret access key are disregarded
    and the profile credentials are used instead.
    """

    def __init__(
        self,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        alternate_ca_bundle: str | None = None,
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
        :param alternate_ca_bundle: The path to an alternate CA bundle file
        :param profile_name: The profile name
        """
        self._aws_session_token = aws_session_token
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_access_key_id = aws_access_key_id
        self._alternate_ca_bundle = alternate_ca_bundle
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
            verify=self._alternate_ca_bundle if self._alternate_ca_bundle else True,
        )

    def to_sqs_client(self, alternate_ca_bundle: str | None = None):
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
            verify=self._alternate_ca_bundle if self._alternate_ca_bundle else True,
        )
