import json
import logging
import uuid
from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class RedisPublisher(IPublishEvents):
    """
    Publishes events over SQS.
    """

    def __init__(
        self,
        redis_url: str,
        topic_map: HierarchicalTopicMap,
        event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str, str]],
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initializes the SqsPublisher.
        :param redis_url: The Redis connection URL.
        :param topic_map: The hierarchical topic map for topic resolution.
        :param event_writer: A callable that writes the event to a message.
        :param logger: Optional logger for logging.
        :raises ValueError: If the event writer cannot determine the topic for the event.
        :raises TypeError: If the event is not a subclass of BaseEvent.
        :raises Exception: If there is an error during message publishing.
        :return: None
        """
        self.__redis_url = redis_url
        self._event_writer = event_writer
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger("SqsPublisher")

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic.
        :param event: The event to publish.
        """

        _, hierarchical_topic, j = self._event_writer(event, self.__topic_map)
        from redis.asyncio import Redis

        async with Redis.from_url(url=self.__redis_url) as redis:
            msg = {
                "message_id": str(uuid.uuid4()),
                "body": j,
                "correlation_id": str(event.correlation_id),
            }
            await redis.publish(hierarchical_topic, json.dumps(msg))
            self.__logger.debug(f"Published {hierarchical_topic}")
