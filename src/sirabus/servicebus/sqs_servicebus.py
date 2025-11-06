import asyncio
import json
import time
from threading import Thread
from typing import Callable, Dict, Optional, Set, Tuple, Iterable

from aett.eventstore import BaseEvent, BaseCommand

from sirabus import (
    IHandleEvents,
    IHandleCommands,
    CommandResponse,
    get_type_param,
)
from sirabus.shared.sqs_config import SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus, ServiceBusConfiguration


class SqsServiceBusConfiguration(ServiceBusConfiguration):
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
        self._sqs_config: Optional[SqsConfig] = None
        self._prefetch_count: int = 10
        import uuid

        self._receive_endpoint_name: str = "sqs_" + str(uuid.uuid4())

    def get_prefetch_count(self) -> int:
        """
        Get the number of messages to prefetch from the SQS queue.
        """
        return self._prefetch_count

    def get_receive_endpoint_name(self) -> str:
        """
        Get the name of the SQS queue to receive messages from.
        :return: The name of the SQS queue.
        :rtype: str
        """
        return self._receive_endpoint_name

    def get_sqs_config(self) -> SqsConfig:
        """
        Get the SQS configuration.
        :raises ValueError: If the SQS config is not set.
        :return: The SQS configuration.
        """
        if not self._sqs_config:
            raise ValueError("SQS config is not set.")
        return self._sqs_config

    def with_prefetch_count(self, prefetch_count: int):
        """
        Set the number of messages to prefetch from the SQS queue.
        :param int prefetch_count: The number of messages to prefetch. Must be >= 1.
        :raises ValueError: If prefetch_count is less than 1.
        :return: The SqsServiceBusConfiguration instance.
        :rtype: SqsServiceBusConfiguration
        """
        if prefetch_count < 1:
            raise ValueError("prefetch_count must be >= 1")
        self._prefetch_count = prefetch_count
        return self

    def with_receive_endpoint_name(self, receive_endpoint_name: str):
        """
        Set the name of the SQS queue to receive messages from. Use this to set a specific queue name.
        If not set, a random name will be generated.
        :param str receive_endpoint_name: The name of the SQS queue.
        :raises ValueError: If receive_endpoint_name is empty.
        :return: The SqsServiceBusConfiguration instance.
        :rtype: SqsServiceBusConfiguration
        """
        if not receive_endpoint_name:
            raise ValueError("receive_endpoint_name must not be empty")
        self._receive_endpoint_name = receive_endpoint_name
        return self

    def with_sqs_config(self, sqs_config: SqsConfig):
        """
        Set the SQS configuration.
        :param SqsConfig sqs_config: The SQS configuration.
        :raises ValueError: If sqs_config is None.
        :return: The SqsServiceBusConfiguration instance.
        :rtype: SqsServiceBusConfiguration
        """
        self._sqs_config = sqs_config
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            read_event,
            write_command_response,
        )

        return SqsServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            read_event,
            write_command_response,
        )

        return SqsServiceBusConfiguration(
            message_reader=read_event,
            command_response_writer=write_command_response,
        )

    @staticmethod
    def for_custom(message_reader, command_response_writer):
        return SqsServiceBusConfiguration(
            message_reader=message_reader,
            command_response_writer=command_response_writer,
        )


class SqsServiceBus(ServiceBus[SqsServiceBusConfiguration]):
    """
    A service bus implementation that uses AWS SQS and SNS for message handling.
    This class allows for the consumption of messages from SQS queues and the publishing of command responses.
    It supports hierarchical topic mapping and can handle both events and commands.
    It is designed to work with AWS credentials and SQS queue configurations provided in the SqsConfig object.
    It also allows for prefetching messages from the SQS queue to improve performance.
    This class is thread-safe and can be used in a multithreaded environment.
    It is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    :note: This class is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    It is thread-safe and can be used in a multithreaded environment.
    It supports hierarchical topic mapping and can handle both events and commands.
    It is designed to work with AWS credentials and SQS queue configurations provided in the SqsConfig object.
    It also allows for prefetching messages from the SQS queue to improve performance.
    """

    def __init__(self, configuration: SqsServiceBusConfiguration) -> None:
        """
        Create a new instance of the SQS service bus consumer class.

        :param SqsServiceBusConfiguration configuration: The SQS service bus configuration.
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
        self.__subscriptions: Set[str] = set()
        self._stopped = False
        self.__sqs_thread: Optional[Thread] = None

    async def run(self):
        self._configuration.get_logger().debug("Starting service bus")
        sns_client = self._configuration.get_sqs_config().to_sns_client()
        sqs_client = self._configuration.get_sqs_config().to_sqs_client()
        declared_queue_response = sqs_client.create_queue(
            QueueName=self._configuration.get_receive_endpoint_name()
        )
        queue_url = declared_queue_response["QueueUrl"]
        queue_attributes = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        relationships = (
            self._configuration.get_topic_map().build_parent_child_relationships()
        )
        topic_hierarchy = set(self._get_topic_hierarchy(self.__topics, relationships))
        for topic in topic_hierarchy:
            self._create_subscription(
                sns_client, topic, queue_attributes["Attributes"]["QueueArn"]
            )
        self.__sqs_thread = Thread(target=self._consume_messages, args=(queue_url,))
        self.__sqs_thread.start()

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

    def _create_subscription(self, sns_client, topic: str, queue_url: str):
        arn = self._configuration.get_topic_map().get_metadata(topic, "arn")
        subscription_response = sns_client.subscribe(
            TopicArn=arn,
            Protocol="sqs",
            Endpoint=queue_url,
        )
        self.__subscriptions.add(subscription_response["SubscriptionArn"])
        self._configuration.get_logger().debug(
            f"Queue {self._configuration.get_receive_endpoint_name()} bound to topic {topic}."
        )

    def _consume_messages(self, queue_url: str):
        """
        Starts consuming messages from the SQS queue.
        :param queue_url: The URL of the SQS queue to consume messages from.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        sqs_client = self._configuration.get_sqs_config().to_sqs_client()
        from botocore.exceptions import EndpointConnectionError
        from urllib3.exceptions import NewConnectionError

        while not self._stopped:
            try:
                response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=self._configuration.get_prefetch_count(),
                    WaitTimeSeconds=3,
                )
            except (
                EndpointConnectionError,
                NewConnectionError,
                ConnectionRefusedError,
            ):
                break
            except Exception as e:
                self._configuration.get_logger().exception(
                    "Error receiving messages from SQS queue", exc_info=e
                )
                time.sleep(self._configuration.get_timeout_seconds())
                continue

            messages = response.get("Messages", [])
            if not messages:
                time.sleep(self._configuration.get_timeout_seconds())
                continue
            for message in messages:
                body = json.loads(message.get("Body", None))
                message_attributes: Dict[str, str] = {}
                for key, value in body.get("MessageAttributes", {}).items():
                    if value["Value"] is not None:
                        message_attributes[key] = value.get("Value", None)
                try:
                    loop.run_until_complete(
                        self._handle_message(
                            headers=message_attributes,
                            body=body.get("Message", None),
                            message_id=body.get("MessageId", None),
                            correlation_id=message_attributes.get(
                                "correlation_id", None
                            )
                            if "correlation_id" in message_attributes
                            else None,
                            reply_to=message_attributes.get("reply_to", None)
                            if "reply_to" in message_attributes
                            else None,
                        )
                    )
                    sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                    )
                except Exception as e:
                    self._configuration.get_logger().exception(
                        f"Error processing message {message['MessageId']}", exc_info=e
                    )

    async def stop(self):
        self._stopped = True

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
        sqs_client = self._configuration.get_sqs_config().to_sqs_client()
        topic, body = self._configuration.write_response(response)
        sqs_client.send_message(
            QueueUrl=reply_to,
            MessageBody=body.decode(),
            MessageAttributes={
                "topic": {
                    "DataType": "String",
                    "StringValue": topic,
                },
                "correlation_id": {
                    "DataType": "String",
                    "StringValue": correlation_id or "",
                },
                "message_id": {
                    "DataType": "String",
                    "StringValue": message_id or "",
                },
            },
        )
