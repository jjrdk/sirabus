import logging

from sirabus import SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


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
