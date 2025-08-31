import json
import uuid
from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from redis.asyncio import Redis
from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.publisher import PublisherConfiguration


class RedisPublisherConfiguration(PublisherConfiguration):
    def __init__(
        self, event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str]]
    ):
        """
        Initializes the Redis Publisher Configuration with the necessary parameters.
        :param event_writer: A callable that formats the event into a message.
        """
        super().__init__(event_writer=event_writer)
        self._redis_url: str | None = None

    def get_redis_url(self) -> str:
        if not self._redis_url:
            raise ValueError("redis_url has not been set.")
        return self._redis_url

    def with_redis_url(self, redis_url: str) -> "RedisPublisherConfiguration":
        if not redis_url:
            raise ValueError("redis_url cannot be None or empty.")
        self._redis_url = redis_url
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import write_event

        return RedisPublisherConfiguration(event_writer=write_event)

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import write_event

        return RedisPublisherConfiguration(event_writer=write_event)


class RedisPublisher(IPublishEvents):
    """
    Publishes events over SQS.
    """

    def __init__(
        self,
        configuration: RedisPublisherConfiguration,
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

        async with self._build_redis_client() as redis:
            msg = {
                "message_id": str(uuid.uuid4()),
                "body": j,
                "correlation_id": str(event.correlation_id),
            }
            await redis.publish(hierarchical_topic, json.dumps(msg))
            self._configuration.get_logger().debug(f"Published {hierarchical_topic}")

    def _build_redis_client(self) -> Redis:
        import urllib3

        url = urllib3.util.parse_url(self._configuration.get_redis_url())
        if not url.host or not url.port:
            raise ValueError("Invalid Redis URL")
        return Redis(
            username=url.auth.split(":")[0] if url.auth else None,
            password=url.auth.split(":")[1] if url.auth else None,
            host=url.host,
            port=url.port,
            ssl=(url.scheme == "rediss"),
            ssl_ca_certs=self._configuration.get_ca_cert_file(),
        )
