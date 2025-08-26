from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.publisher import PublisherConfiguration
from sirabus.topography.sqs import SqsConfig


class SqsPublisherConfiguration(PublisherConfiguration):
    def __init__(
        self, event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str]]
    ):
        """
        Initializes the SQS Publisher Configuration with the necessary parameters.
        :param event_writer: A callable that formats the event into a message.
        """
        super().__init__(event_writer=event_writer)
        self._sqs_config: SqsConfig | None = None

    def get_sqs_config(self) -> SqsConfig:
        if not self._sqs_config:
            raise ValueError("sqs_config has not been set.")
        return self._sqs_config

    def with_sqs_config(self, sqs_config: SqsConfig) -> "SqsPublisherConfiguration":
        if not sqs_config:
            raise ValueError("sqs_config cannot be None.")
        self._sqs_config = sqs_config
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import write_event

        return SqsPublisherConfiguration(event_writer=write_event)

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import write_event

        return SqsPublisherConfiguration(event_writer=write_event)


class SqsPublisher(IPublishEvents):
    """
    Publishes events over SQS.
    """

    def __init__(
        self,
        configuration: SqsPublisherConfiguration,
    ) -> None:
        """
        Initializes the SqsPublisher.
        :param configuration: The publisher configuration.
        :raises ValueError: If the event writer cannot determine the topic for the event.
        :raises TypeError: If the event is not a subclass of BaseEvent.
        :raises Exception: If there is an error during message publishing.
        :return: None
        """
        self._configuration = configuration

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic.
        :param event: The event to publish.
        """

        hierarchical_topic, j = self._configuration.write_event(event)
        sns_client = self._configuration.get_sqs_config().to_sns_client()
        import json

        metadata = self._configuration.get_topic_map().get_metadata(
            hierarchical_topic, "arn"
        )
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
        self._configuration.get_logger().debug(f"Published {hierarchical_topic}")
