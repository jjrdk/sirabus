import asyncio
import json
import logging
from typing import Dict, List, Optional, Set, Callable, Tuple, Iterable

from aett.eventstore import BaseEvent

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus


class RedisServiceBus(ServiceBus):
    """
    A service bus implementation that uses Redis for message handling.
    This class allows for the consumption of messages from Redis PubSuband the publishing of command responses.
    It supports hierarchical topic mapping and can handle both events and commands.
    This class is thread-safe and can be used in a multi-threaded environment.
    It is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    :param str redis_url: The URL of the Redis instance to use.
    :param HierarchicalTopicMap topic_map: The topic map to use for resolving topics.
    :param List[IHandleEvents | IHandleCommands] handlers: The list of event and command handlers to register.
    :param Callable message_reader: Function to deserialize messages from Redis.
    :param Callable command_response_writer: Function to serialize command responses for Redis.
    :param Optional[logging.Logger] logger: Logger instance to use for logging.
    :raises ValueError: If the message reader cannot determine the topic for the event or command.
    :raises TypeError: If the event or command is not a subclass of BaseEvent or BaseCommand.
    :raises Exception: If there is an error during message handling or response sending.
    :return: None
    :raises RuntimeError: If the service bus cannot be started or stopped.
    :raises Exception: If there is an error during message processing or cleanup.
    :note: This class is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    It is thread-safe and can be used in a multi-threaded environment.
    It supports hierarchical topic mapping and can handle both events and commands.
    """

    def __init__(
        self,
        redis_url: str,
        topic_map: HierarchicalTopicMap,
        handlers: List[IHandleEvents | IHandleCommands],
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Create a new instance of the Redis service bus consumer class.

        :param redis_url: The URL of the Redis instance to use.
        :param HierarchicalTopicMap topic_map: The topic map to use for resolving topics.
        :param List[IHandleEvents | IHandleCommands] handlers: The list of event and command handlers to register.
        :param Callable message_reader: Function to deserialize messages from Redis.
        :param Callable command_response_writer: Function to serialize command responses for Redis.
        :param Optional[logging.Logger] logger: Logger instance to use for logging.
        """
        super().__init__(
            message_reader=message_reader,
            topic_map=topic_map,
            handlers=handlers,
            logger=logger or logging.getLogger("RedisServiceBus"),
        )
        from redis.asyncio import Redis

        self.__redis_client = Redis.from_url(url=redis_url)
        self.__redis_pubsub = self.__redis_client.pubsub()
        self.__command_response_writer = command_response_writer
        self._topic_map = topic_map
        self._logger = logger or logging.getLogger("ServiceBus")
        self.__topics = set(
            topic
            for topic in (
                self._topic_map.get_from_type(get_type_param(handler))
                for handler in handlers
                if isinstance(handler, (IHandleEvents, IHandleCommands))
            )
            if topic is not None
        )
        self._stopped = False
        self.__read_task: Optional[asyncio.Task] = None

    async def run(self):
        self._logger.debug("Starting service bus")

        relationships = self._topic_map.build_parent_child_relationships()
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
                    await self.handle_message(
                        headers={"topic": message["channel"].decode()},
                        body=data.get("body", b""),
                        message_id=data.get("message_id", None),
                        correlation_id=data.get("correlation_id", None),
                        reply_to=data.get("reply_to", None),
                    )
            except Exception as e:
                self._logger.error(f"Failed to consume message", exc_info=e)

    async def stop(self):
        self._stopped = True
        if self.__read_task:
            self.__read_task.cancel()
            try:
                await self.__read_task
            except asyncio.CancelledError:
                pass

    async def send_command_response(
        self,
        response: CommandResponse,
        message_id: str | None,
        correlation_id: str | None,
        reply_to: str,
    ):
        self._logger.debug(
            f"Response published to {reply_to} with correlation_id {correlation_id}."
        )
        _, body = self.__command_response_writer(response)
        msg = {
            "message_id": message_id,
            "correlation_id": correlation_id,
            "body": body.decode(),
        }
        await self.__redis_client.publish(channel=reply_to, message=json.dumps(msg))
