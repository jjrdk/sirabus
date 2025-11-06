import asyncio
from threading import Thread
from typing import Dict, Tuple, Optional, Callable, Self
from uuid import uuid4

from aett.eventstore.base_command import BaseCommand

from sirabus import CommandResponse, IRouteCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.router import RouterConfiguration
from sirabus.shared.pubsub_config import PubSubConfig


class PubSubRouterConfiguration(RouterConfiguration):
    """Configuration for PubSub Command Router."""

    def __init__(
        self,
        message_writer: Callable[[BaseCommand, HierarchicalTopicMap], Tuple[str, str]],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
    ) -> None:
        """
        Initializes the PubSubRouterConfiguration with the necessary parameters.
        :param Callable message_writer: A callable that formats the command into a message.
        :param Callable response_reader: A callable that reads the response from the message.
        :raises ValueError: If the amqp_url is empty.
        """
        super().__init__(message_writer=message_writer, response_reader=response_reader)
        self._pubsub_config: Optional[PubSubConfig] = None

    def get_pubsub_config(self) -> PubSubConfig:
        """
        Gets the PubSub configuration.
        :return: The PubSub configuration.
        :raises ValueError: If the PubSub_config is not set.
        """
        if not self._pubsub_config:
            raise ValueError("PubSub_config has not been set.")
        return self._pubsub_config

    def with_pubsub_config(self, config: PubSubConfig) -> Self:
        """
        Sets the PubSub configuration.
        :param PubSubConfig config: The PubSub configuration to set.
        :return: The PubSubRouterConfiguration instance.
        :raises ValueError: If the PubSub_config is None.
        """
        if not config:
            raise ValueError("config cannot be None.")
        self._pubsub_config = config
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            write_command,
            read_command_response,
        )

        return PubSubRouterConfiguration(
            message_writer=write_command, response_reader=read_command_response
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            write_command,
            read_command_response,
        )

        return PubSubRouterConfiguration(
            message_writer=write_command, response_reader=read_command_response
        )


class PubSubCommandRouter(IRouteCommands):
    def __init__(self, configuration: PubSubRouterConfiguration) -> None:
        """
        Initializes the PubSubCommandRouter.
        :param PubSubRouterConfiguration configuration: The PubSub router configuration.
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
        loop = asyncio.get_event_loop()
        try:
            hierarchical_topic, j = self._configuration.write_message(command)
        except ValueError:
            future = loop.create_future()
            future.set_result(CommandResponse(success=False, message="unknown command"))
            return future
        async with (
            self._configuration.get_pubsub_config().to_publisher_client() as pubsub_client
        ):
            topic_path = pubsub_client.topic_path(
                project=self._configuration.get_pubsub_config().get_project_id(),
                topic=f"PubSub_{str(uuid4())}",
            )
            declared_queue_response = await pubsub_client.create_topic(
                name=topic_path, metadata=[("temporary", "true")]
            )
            consume_thread = Thread(
                target=asyncio.run,
                args=(self._consume_queue(declared_queue_response.name),),
            )
            consume_thread.start()

            metadata = self._configuration.get_topic_map().get_metadata(
                hierarchical_topic, "pubsub_topic"
            )
            from sirabus.shared.pubsub import create_pubsub_message

            response = await pubsub_client.publish(
                topic=metadata,
                messages=[
                    create_pubsub_message(
                        data=j.encode(),
                        hierarchical_topic=hierarchical_topic,
                        correlation_id=command.correlation_id,
                        reply_to=declared_queue_response.name,
                    )
                ],
            )
            message_id = response.message_ids[0]
            self._configuration.get_logger().debug(f"Published {hierarchical_topic}")
            future = loop.create_future()
            self.__inflight[message_id] = (future, consume_thread)
            return future

    async def _consume_queue(self, queue_url: str) -> None:
        async with (
            self._configuration.get_pubsub_config().to_subscriber_client() as subscriber_client
        ):
            response_received = False
            subscription = await subscriber_client.create_subscription(
                name=f"projects/{self._configuration.get_pubsub_config().get_project_id()}/subscriptions/response_{uuid4()}",
                topic=queue_url,
                ack_deadline_seconds=60,
            )
            while not response_received:
                pull_response = await subscriber_client.pull(
                    subscription=subscription.name,
                    max_messages=1,
                    return_immediately=True,
                    timeout=self._configuration.get_timeout_seconds(),
                )
                for msg in pull_response.received_messages:
                    message_attributes: Dict[str, str] = {
                        key: value for key, value in msg.message.attributes.items()
                    }
                    response = self._configuration.read_response(
                        message_attributes, msg.message.data
                    )
                    if not response:
                        response = CommandResponse(
                            success=False, message="No response received."
                        )
                    try:
                        future, _ = self.__inflight.get(
                            message_attributes["message_id"], None
                        )
                        if future and not future.done():
                            future.set_result(response)
                            response_received = True
                            _ = await subscriber_client.acknowledge(
                                subscription=subscription.name, ack_ids=[msg.ack_id]
                            )
                    except Exception as e:
                        self._configuration.get_logger().exception(
                            f"Error deleting message {msg.message.message_id}",
                            exc_info=e,
                        )
            await subscriber_client.delete_subscription(subscription=subscription.name)
