import logging

from sirabus import SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class TopographyBuilder:
    """
    Builds the topography for the SQS service bus by creating topics according to the hierarchical topic map.
    This class is responsible for creating SNS topics based on the provided topic map.
    It uses the SqsConfig to create an SNS client and then creates topics for each entry in the topic map.
    It sets the ARN of each created topic in the topic map metadata.
    :param topic_map: The hierarchical topic map for topic resolution.
    :param config: The SqsConfig containing AWS credentials and configuration.
    :param logger: Optional logger for logging.
    :raises ValueError: If the topic map is not provided or if a topic creation fails.
    :raises TypeError: If the topic map is not an instance of HierarchicalTopicMap.
    :raises Exception: If there is an error during topic creation or if the ARN is not returned.
    """

    def __init__(
        self,
        topic_map: HierarchicalTopicMap,
        config: SqsConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initializes the TopographyBuilder with the topic map and SqsConfig.
        :param topic_map: The hierarchical topic map for topic resolution.
        :param config: The SqsConfig containing AWS credentials and configuration.
        :param logger: Optional logger for logging.
        :raises ValueError: If the topic map is not provided.
        :raises TypeError: If the topic map is not an instance of HierarchicalTopicMap.
        :raises Exception: If there is an error during topic creation or if the ARN is not returned.
        """
        self.__config = config
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger(__name__)

    def build(self):
        """
        Builds the topography by creating SNS topics for each entry in the topic map.
        This method connects to the AWS SNS service using the provided SqsConfig and creates topics
        for each topic in the hierarchical topic map. It sets the ARN of each created topic in
        the topic map metadata.
        """
        client = self.__config.to_sns_client()
        for topic in self.__topic_map.get_all():
            topic_name = topic.replace(".", "_")
            topic_response = client.create_topic(Name=topic_name)
            topic_arn = topic_response.get("TopicArn")
            self.__topic_map.set_metadata(topic, "arn", topic_arn)
            if not topic_arn:
                raise ValueError(
                    f"Failed to create topic {topic_name}. No ARN returned."
                )
            self.__logger.debug(f"Queue {topic_name} created.")
