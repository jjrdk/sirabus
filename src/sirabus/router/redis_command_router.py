import asyncio
from threading import Thread
from typing import Dict, Tuple, Optional, Callable
from uuid import uuid4

from aett.eventstore.base_command import BaseCommand
from redis.asyncio import Redis

from sirabus import CommandResponse, IRouteCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.router import RouterConfiguration


class RedisRouterConfiguration(RouterConfiguration):
    """Configuration for Redis Command Router."""

    def __init__(
        self,
        message_writer: Callable[[BaseCommand, HierarchicalTopicMap], Tuple[str, str]],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
    ) -> None:
        """
        Initializes the RedisRouterConfiguration with the necessary parameters.
        :param Callable message_writer: A callable that formats the command into a message.
        :param Callable response_reader: A callable that reads the response from the message.
        """
        super().__init__(message_writer=message_writer, response_reader=response_reader)
        self._redis_url: Optional[str] = None

    def get_redis_url(self) -> str:
        """
        Gets the Redis URL.
        :return: The Redis URL.
        :raises ValueError: If the redis_url is not set.
        """
        if not self._redis_url:
            raise ValueError("redis_url has not been set.")
        return self._redis_url

    def with_redis_url(self, redis_url: str) -> "RedisRouterConfiguration":
        """
        Sets the Redis URL.
        :param str redis_url: The Redis URL to set.
        :return: The RedisRouterConfiguration instance.
        :raises ValueError: If the redis_url is None or empty.
        """
        if not redis_url:
            raise ValueError("redis_url cannot be None or empty.")
        self._redis_url = redis_url
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            write_command,
            read_command_response,
        )

        return RedisRouterConfiguration(
            message_writer=write_command, response_reader=read_command_response
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            write_command,
            read_command_response,
        )

        return RedisRouterConfiguration(
            message_writer=write_command, response_reader=read_command_response
        )


class RedisCommandRouter(IRouteCommands):
    def __init__(
        self,
        configuration: RedisRouterConfiguration,
    ) -> None:
        """
        Initializes the SqsCommandRouter.
        :param RedisRouterConfiguration configuration: The configuration for the router.
        :raises ValueError: If the message writer cannot determine the topic for the command.
        :raises TypeError: If the response reader does not return a CommandResponse or None.
        :raises Exception: If there is an error during message publishing or response handling.
        :return: None
        """
        self._configuration = configuration
        self.__inflight: Dict[str, Tuple[asyncio.Future[CommandResponse], Thread]] = {}

    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        loop = asyncio.get_running_loop()
        try:
            hierarchical_topic, j = self._configuration.write_message(command)
        except ValueError:
            future = loop.create_future()
            future.set_result(CommandResponse(success=False, message="unknown command"))
            return future

        msg_id = str(uuid4())
        reply_to = str(uuid4())
        msg = {
            "body": j,
            "message_id": msg_id,
            "correlation_id": command.correlation_id,
            "reply_to": reply_to,
        }
        consume_thread = Thread(
            target=asyncio.run, args=(self._consume_queue(reply_to),)
        )
        consume_thread.start()
        import json

        async with self._build_redis_client() as client:
            await client.publish(channel=hierarchical_topic, message=json.dumps(msg))
        self._configuration.get_logger().debug(f"Published {hierarchical_topic}")
        future = loop.create_future()
        self.__inflight[msg_id] = (future, consume_thread)
        return future

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

    async def _consume_queue(self, reply_to: str) -> None:
        response_received = False
        async with self._build_redis_client() as client:
            async with client.pubsub() as pubsub:
                await pubsub.subscribe(reply_to)
                while not response_received:
                    try:
                        message = await pubsub.get_message(
                            ignore_subscribe_messages=True
                        )
                        if not message:
                            await asyncio.sleep(0.1)
                            continue
                    except Exception as e:
                        self._configuration.get_logger().exception(
                            "Error receiving messages from SQS queue", exc_info=e
                        )
                        continue

                    import json

                    data = json.loads(message["data"])
                    message_id = data.get("message_id")
                    topic = message["channel"].decode()
                    response = self._configuration.read_response(
                        {"topic": topic}, data.get("body", b"")
                    )
                    if not response:
                        response = CommandResponse(
                            success=False, message="No response received."
                        )
                    try:
                        future, _ = self.__inflight.get(message_id, None)
                        if future and not future.done():
                            future.set_result(response)
                            response_received = True
                    except Exception as e:
                        self._configuration.get_logger().exception(
                            f"Error consuming message {message_id}",
                            exc_info=e,
                        )
