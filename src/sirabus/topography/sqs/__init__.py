import logging
from boto3 import Session

from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class SqsConfig:
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
        Defines the configuration for the S3 client.
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


class TopographyBuilder:
    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        config: SqsConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        self.__config = config
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger(__name__)

    def build(self):
        client = self.__config.to_sns_client()
        for topic in self.__topic_map.get_all_hierarchical_topics():
            topic_name = topic.replace(".", "_")
            topic_response = client.create_topic(Name=topic_name)
            topic_arn = topic_response.get("TopicArn")
            self.__topic_map.set_metadata(topic, "arn", topic_arn)
            if not topic_arn:
                raise ValueError(
                    f"Failed to create topic {topic_name}. No ARN returned."
                )
            self.__logger.debug(f"Queue {topic_name} created.")
