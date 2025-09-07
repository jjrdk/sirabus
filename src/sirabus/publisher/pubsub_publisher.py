from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.publisher import PublisherConfiguration
from sirabus.shared.pubsub_config import PubSubConfig


class PubSubPublisherConfiguration(PublisherConfiguration):
    def __init__(
        self, event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str]]
    ):
        """
        Initializes the PubSub Publisher Configuration with the necessary parameters.
        :param event_writer: A callable that formats the event into a message.
        """
        super().__init__(event_writer=event_writer)
        self._pubsub_config: PubSubConfig | None = None

    def get_pubsub_config(self) -> PubSubConfig:
        if not self._pubsub_config:
            raise ValueError("pubsub_config has not been set.")
        return self._pubsub_config

    def with_pubsub_config(
        self, pubsub_config: PubSubConfig
    ) -> "PubSubPublisherConfiguration":
        if not pubsub_config:
            raise ValueError("pubsub_config cannot be None.")
        self._pubsub_config = pubsub_config
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import write_event

        return PubSubPublisherConfiguration(event_writer=write_event)

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import write_event

        return PubSubPublisherConfiguration(event_writer=write_event)


class PubSubPublisher(IPublishEvents):
    """
    Publishes events over GCP PubSub.
    """

    def __init__(
        self,
        configuration: PubSubPublisherConfiguration,
    ) -> None:
        """
        Initializes the PubSubPublisher.
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
        async with (
            self._configuration.get_pubsub_config().to_publisher_client() as client
        ):
            pubsub_topic = self._configuration.get_topic_map().get_metadata(
                hierarchical_topic, "pubsub_topic"
            )
            from sirabus.shared.pubsub import create_pubsub_message

            response = await client.publish(
                topic=pubsub_topic,
                messages=[
                    create_pubsub_message(
                        data=j.encode(),
                        hierarchical_topic=hierarchical_topic,
                        correlation_id=event.correlation_id,
                    )
                ],
                metadata=[
                    ("correlation_id", event.correlation_id or ""),
                    ("topic", hierarchical_topic),
                ],
            )
            self._configuration.get_logger().debug(
                f"Published {hierarchical_topic} with id {response.message_ids[0]}"
            )
