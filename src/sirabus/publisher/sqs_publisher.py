import logging
from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.topography.sqs import SqsConfig


class SqsPublisher(IPublishEvents):
    """
    Publishes events over SQS.
    """

    def __init__(
        self,
        sqs_config: SqsConfig,
        topic_map: HierarchicalTopicMap,
        event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str, str]],
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initializes the SqsPublisher.
        :param sqs_config: The SQS configuration.
        :param topic_map: The hierarchical topic map for topic resolution.
        :param event_writer: A callable that writes the event to a message.
        :param logger: Optional logger for logging.
        :raises ValueError: If the event writer cannot determine the topic for the event.
        :raises TypeError: If the event is not a subclass of BaseEvent.
        :raises Exception: If there is an error during message publishing.
        :return: None
        """
        self.__sqs_config = sqs_config
        self._event_writer = event_writer
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger("SqsPublisher")

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic.
        :param event: The event to publish.
        """

        _, hierarchical_topic, j = self._event_writer(event, self.__topic_map)
        sns_client = self.__sqs_config.to_sns_client()
        import json

        metadata = self.__topic_map.get_metadata(hierarchical_topic, "arn")
        _ = sns_client.publish(
            TopicArn=metadata,
            Message=json.dumps({"default": j}),
            Subject=hierarchical_topic,
            MessageStructure="json",
            MessageAttributes={
                "correlation_id": {
                    "StringValue": event.correlation_id,
                    "DataType": "String",
                },
                "topic": {
                    "StringValue": hierarchical_topic,
                    "DataType": "String",
                },
            },
        )
        self.__logger.debug(f"Published {hierarchical_topic}")


def create_publisher_for_sqs(
    config: SqsConfig,
    topic_map: HierarchicalTopicMap,
    logger: logging.Logger | None = None,
) -> IPublishEvents:
    """
    Creates a CloudEventPublisher for SQS.
    :param config: The SQS configuration.
    :param topic_map: The hierarchical topic map.
    :param logger: Optional logger.
    :return: A CloudEventPublisher instance.
    """
    from sirabus.publisher.cloudevent_serialization import create_event

    return SqsPublisher(
        sqs_config=config, topic_map=topic_map, event_writer=create_event, logger=logger
    )
