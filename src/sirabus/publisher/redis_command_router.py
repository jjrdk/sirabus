import asyncio
import logging
from threading import Thread
from uuid import uuid4
from typing import Dict, Tuple, Optional, Callable

from redis.asyncio import Redis
from aett.eventstore.base_command import BaseCommand
from sirabus import CommandResponse, IRouteCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class RedisCommandRouter(IRouteCommands):
    def __init__(
        self,
        redis_url: str,
        topic_map: HierarchicalTopicMap,
        message_writer: Callable[
            [BaseCommand, HierarchicalTopicMap], Tuple[str, str, str]
        ],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initializes the SqsCommandRouter.
        :param redis_url: Redis URL for Pub/Sub.
        :param topic_map: The hierarchical topic map for topic resolution.
        :param message_writer: A callable that writes the command to a message.
        :param response_reader: A callable that reads the response from a message.
        :param logger: Optional logger for logging.
        :raises ValueError: If the message writer cannot determine the topic for the command.
        :raises TypeError: If the response reader does not return a CommandResponse or None.
        :raises Exception: If there is an error during message publishing or response handling.
        :return: None
        """
        self.__response_reader = response_reader
        self.__message_writer = message_writer
        self.__inflight: Dict[str, Tuple[asyncio.Future[CommandResponse], Thread]] = {}
        self.__redis_url = redis_url
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger("RedisCommandRouter")

    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        loop = asyncio.get_running_loop()
        try:
            _, hierarchical_topic, j = self.__message_writer(command, self.__topic_map)
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

        async with Redis.from_url(url=self.__redis_url) as client:
            await client.publish(channel=hierarchical_topic, message=json.dumps(msg))
        self.__logger.debug(f"Published {hierarchical_topic}")
        future = loop.create_future()
        self.__inflight[msg_id] = (future, consume_thread)
        return future

    async def _consume_queue(self, reply_to: str) -> None:
        response_received = False
        async with Redis.from_url(url=self.__redis_url) as client:
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
                        self.__logger.exception(
                            "Error receiving messages from SQS queue", exc_info=e
                        )
                        continue

                    import json

                    data = json.loads(message["data"])
                    message_id = data.get("message_id")
                    topic = message["channel"].decode()
                    response = self.__response_reader(
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
                        self.__logger.exception(
                            f"Error consuming message {message_id}",
                            exc_info=e,
                        )
