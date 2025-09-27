import asyncio
import json
from typing import Callable, Optional, Tuple
from typing import Dict, Iterable, Set

from aett.eventstore import BaseEvent, BaseCommand
from redis.asyncio import Redis, ConnectionPool

from sirabus import CommandResponse
from sirabus import IHandleEvents, IHandleCommands, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus, ServiceBusConfiguration


class RedisServiceBusConfiguration(ServiceBusConfiguration):
    def __init__(
        self,
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
    ):
        super().__init__(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )
        self._redis_url: Optional[str] = None

    def get_redis_url(self) -> str:
        """
        Get the Redis URL.
        :return: The Redis URL.
        :rtype: str
        :raises ValueError: If the Redis URL is not set.
        """
        if not self._redis_url:
            raise ValueError("Redis URL is not set.")
        return self._redis_url

    def with_redis_url(self, redis_url: str):
        """
        Set the Redis URL.
        :param redis_url: The Redis URL.
        :return: The Redis service bus configuration.
        :rtype: RedisServiceBusConfiguration
        :raises ValueError: If the Redis URL is empty.
        """
        if not redis_url or redis_url == "":
            raise ValueError("redis_url must not be empty")
        self._redis_url = redis_url
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            read_event,
            write_command_response,
        )

        return RedisServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            read_event,
            write_command_response,
        )

        return RedisServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_custom(message_reader, command_response_writer):
        return RedisServiceBusConfiguration(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )


class RedisServiceBus(ServiceBus[RedisServiceBusConfiguration]):
    """
    A service bus implementation that uses Redis for message handling.
    This class allows for the consumption of messages from Redis PubSub and the publishing of command responses.
    It supports hierarchical topic mapping and can handle both events and commands.
    This class is thread-safe and can be used in a multithreaded environment.
    It is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    :note: This class is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    It is thread-safe and can be used in a multithreaded environment.
    It supports hierarchical topic mapping and can handle both events and commands.
    """

    def __init__(
        self,
        configuration: RedisServiceBusConfiguration,
    ) -> None:
        """
        Create a new instance of the Redis service bus consumer class.
        :param RedisServiceBusConfiguration configuration: The Redis service bus configuration.
        """
        super().__init__(configuration=configuration)

        self.__redis_client = self._build_redis_client()
        self.__redis_pubsub = self.__redis_client.pubsub()
        self.__topics = set(
            topic
            for topic in (
                self._configuration.get_topic_map().get_from_type(
                    get_type_param(handler)
                )
                for handler in configuration.get_handlers()
                if isinstance(handler, (IHandleEvents, IHandleCommands))
            )
            if topic is not None
        )
        self._stopped = False
        self.__read_task: Optional[asyncio.Task] = None

    def _build_redis_client(self) -> Redis:
        import urllib3

        redis_url = self._configuration.get_redis_url()
        url = urllib3.util.parse_url(redis_url)
        if url.scheme == "redis":
            return Redis.from_url(redis_url)
        if not url.host or not url.port:
            raise ValueError("Invalid Redis URL")
        return Redis(
            single_connection_client=False,
            username=url.auth.split(":")[0] if url.auth else None,
            password=url.auth.split(":")[1] if url.auth else None,
            host=url.host,
            port=url.port,
            ssl=True,
            ssl_ca_certs=self._configuration.get_ca_cert_file(),
        )

    async def run(self):
        self._configuration.get_logger().debug("Starting Redis service bus")

        relationships = (
            self._configuration.get_topic_map().build_parent_child_relationships()
        )
        topic_hierarchy = set(self._get_topic_hierarchy(self.__topics, relationships))
        await self.__redis_pubsub.subscribe(*topic_hierarchy)
        self.__read_task = asyncio.create_task(
            self._consume_messages(),
        )

    def _get_topic_hierarchy(
        self, topics: Set[str], relationships: Dict[str, Set[str]]
    ) -> Iterable[str]:
        """
        Returns the hierarchy of topics for the given set of topics.
        :param topics: The set of topics to get the hierarchy for.
        :param relationships: The relationships between topics.
        :return: An iterable of topic names in the hierarchy.
        """
        for topic in topics:
            yield from self._get_child_hierarchy(topic, relationships)

    def _get_child_hierarchy(
        self, topic: str, relationships: Dict[str, Set[str]]
    ) -> Iterable[str]:
        children = relationships.get(topic, set())
        if any(children):
            yield from self._get_topic_hierarchy(children, relationships)
        yield topic

    async def _consume_messages(self):
        """
        Starts consuming messages from the Redis PubSub.
        """
        while not self._stopped:
            try:
                message = await self.__redis_pubsub.get_message(
                    ignore_subscribe_messages=True
                )
                if message is not None:
                    data = json.loads(message["data"])
                    await self._handle_message(
                        headers={"topic": message["channel"].decode()},
                        body=data.get("body", b""),
                        message_id=data.get("message_id", None),
                        correlation_id=data.get("correlation_id", None),
                        reply_to=data.get("reply_to", None),
                    )
            except Exception as e:
                self._configuration.get_logger().error(
                    f"Failed to consume message", exc_info=e
                )

    async def stop(self):
        self._stopped = True
        if self.__read_task:
            self.__read_task.cancel()
            try:
                await self.__read_task
            except asyncio.CancelledError:
                pass

    async def _send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ):
        self._configuration.get_logger().debug(
            f"Response published to {reply_to} with correlation_id {correlation_id}."
        )
        _, body = self._configuration.write_response(response)
        msg = {
            "message_id": message_id,
            "correlation_id": correlation_id,
            "body": body.decode(),
        }
        await self.__redis_client.publish(channel=reply_to, message=json.dumps(msg))
