import asyncio
from typing import (
    Callable,
    Dict,
    Optional,
    Self,
    Set,
    Tuple,
    Iterable,
)

from aett.eventstore import BaseEvent, BaseCommand
from google.api_core import exceptions as gcp_exceptions

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus, ServiceBusConfiguration
from sirabus.shared.pubsub_config import PubSubConfig


class PubSubServiceBusConfiguration(ServiceBusConfiguration):
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
        self._prefetch_count = 10
        self._pubsub_config: Optional[PubSubConfig] = None
        import uuid

        self._receive_endpoint_name: str = "pubsub_" + str(uuid.uuid4())

    def get_pubsub_config(self) -> PubSubConfig:
        if self._pubsub_config is None:
            raise ValueError("PubSubConfig is not set.")
        return self._pubsub_config

    def get_prefetch_count(self) -> int:
        return self._prefetch_count

    def with_pubsub_config(self, pubsub_config: PubSubConfig) -> Self:
        self._pubsub_config = pubsub_config
        return self

    def with_prefetch_count(self, prefetch_count: int) -> Self:
        self._prefetch_count = prefetch_count
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            read_event,
            write_command_response,
        )

        return PubSubServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            read_event,
            write_command_response,
        )

        return PubSubServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_custom(message_reader, command_response_writer):
        return PubSubServiceBusConfiguration(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )


class PubSubServiceBus(ServiceBus[PubSubServiceBusConfiguration]):
    """
    A service bus implementation that uses GCP PubSub for message handling.
    This class allows for the consumption of messages from GCP PubSub and the publishing of command responses.
    It supports hierarchical topic mapping and can handle both events and commands.
    It is designed to work with GCP credentials and PubSub configurations provided in the PubSubConfig object.
    This class is thread-safe and can be used in a multi-threaded environment.
    It is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    :note: This class is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    It is thread-safe and can be used in a multithreaded environment.
    It supports hierarchical topic mapping and can handle both events and commands.
    It is designed to work with GCP credentials and PubSub topic configurations provided in the PubSubConfig object.
    It also allows for prefetching messages from the PubSub topic to improve performance.
    """

    def __init__(self, configuration: PubSubServiceBusConfiguration) -> None:
        """
        Create a new instance of the PubSub service bus consumer class.

        :param PubSubServiceBusConfiguration configuration: The PubSub service bus configuration.
        """
        super().__init__(configuration=configuration)
        self.__topics = set(
            topic
            for topic in (
                self._configuration.get_topic_map().get_from_type(
                    get_type_param(handler)
                )
                for handler in self._configuration.get_handlers()
                if isinstance(handler, (IHandleEvents, IHandleCommands))
            )
            if topic is not None
        )
        self._configuration = configuration
        self.__subscriptions: Set[str] = set()
        self._stopped = False
        self.__read_task: Optional[asyncio.Task] = None

    async def run(self):
        relationships = (
            self._configuration.get_topic_map().build_parent_child_relationships()
        )
        topic_hierarchy = set(self._get_topic_hierarchy(self.__topics, relationships))
        topics_to_subscribe = set()
        for topic in topic_hierarchy:
            pubsub_topic = self._configuration.get_topic_map().get_metadata(
                topic, "pubsub_topic"
            )
            topics_to_subscribe.add((topic, pubsub_topic))

        self.__subscriptions = await self._create_subscriptions(topics_to_subscribe)
        self.__read_task = asyncio.create_task(self._consume_messages())

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

    async def _create_subscriptions(
        self, topics_to_subscribe: Set[Tuple[str, str]]
    ) -> Set[str]:
        subscriptions = set()
        async with (
            self._configuration.get_pubsub_config().to_subscriber_client() as subscriber_client
        ):
            for topic_name, pubsub_topic in topics_to_subscribe:
                subscription_name = (
                    f"projects/{self._configuration.get_pubsub_config().get_project_id()}"
                    f"/subscriptions/{topic_name}"
                )
                try:
                    subscription = await subscriber_client.create_subscription(
                        name=subscription_name,
                        topic=pubsub_topic,
                        ack_deadline_seconds=60,
                    )
                    subscriptions.add(subscription.name)
                    self._configuration.get_logger().debug(
                        f"Subscription {subscription.name} created for topic {topic_name}."
                    )
                except Exception as e:
                    self._configuration.get_logger().exception(
                        f"Error creating subscription for topic {topic_name}: {e}"
                    )
                    raise
        return subscriptions
    async def _consume_messages(self):
        """
        Starts consuming messages from the PubSub subscriptions.
        """
        async with (
            self._configuration.get_pubsub_config().to_subscriber_client() as subscriber_client
        ):
            while not self._stopped:
                if not self.__subscriptions:
                    await asyncio.sleep(0.1)
                    continue
                for subscription in self.__subscriptions:
                    if self._stopped:
                        break
                    try:
                        response = await subscriber_client.pull(
                            subscription=subscription,
                            return_immediately=True,
                            max_messages=self._configuration.get_prefetch_count(),
                            timeout=self._configuration.get_timeout_seconds(),
                            retry=None,
                        )
                    except (
                        gcp_exceptions.ServiceUnavailable,
                        gcp_exceptions.RetryError,
                    ) as e:
                        if self._stopped:
                            break
                        self._configuration.get_logger().debug(
                            f"Error pulling messages from subscription {subscription}: {e}"
                        )
                        continue

                    await asyncio.gather(
                        *(
                            self._handle_message(
                                headers={
                                    key: value
                                    for key, value in msg.message.attributes.items()
                                },
                                body=msg.message.data,
                                correlation_id=msg.message.attributes.get(
                                    "correlation_id", None
                                ),
                                reply_to=msg.message.attributes.get("reply_to", None),
                                message_id=msg.message.message_id,
                            )
                            for msg in response.received_messages
                        )
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
        publisher_client = self._configuration.get_pubsub_config().to_publisher_client()
        topic, body = self._configuration.write_response(response)
        from sirabus.shared.pubsub import create_pubsub_message

        await publisher_client.publish(
            topic=reply_to,
            messages=[
                create_pubsub_message(
                    data=body,
                    hierarchical_topic=topic,
                    correlation_id=correlation_id,
                    message_id=message_id,
                )
            ],
        )
