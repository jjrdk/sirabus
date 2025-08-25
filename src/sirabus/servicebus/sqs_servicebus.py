import asyncio
import json
import time
from threading import Thread
from typing import Dict, Optional, Set, Iterable

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.servicebus import ServiceBus, SqsServiceBusConfiguration


class SqsServiceBus(ServiceBus[SqsServiceBusConfiguration]):
    """
    A service bus implementation that uses AWS SQS and SNS for message handling.
    This class allows for the consumption of messages from SQS queues and the publishing of command responses.
    It supports hierarchical topic mapping and can handle both events and commands.
    It is designed to work with AWS credentials and SQS queue configurations provided in the SqsConfig object.
    It also allows for prefetching messages from the SQS queue to improve performance.
    This class is thread-safe and can be used in a multi-threaded environment.
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
                time.sleep(1)
                continue

            messages = response.get("Messages", [])
            if not messages:
                time.sleep(1)
                continue
            for message in messages:
                body = json.loads(message.get("Body", None))
                message_attributes: Dict[str, str] = {}
                for key, value in body.get("MessageAttributes", {}).items():
                    if value["Value"] is not None:
                        message_attributes[key] = value.get("Value", None)
                try:
                    loop.run_until_complete(
                        self.handle_message(
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
