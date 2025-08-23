import asyncio
import json
import logging
import time
from threading import Thread
from typing import Dict, List, Optional, Set, Callable, Tuple, Iterable

from aett.eventstore import BaseEvent

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus
from sirabus.topography.sqs import SqsConfig


class SqsServiceBus(ServiceBus):
    """
    A service bus implementation that uses AWS SQS and SNS for message handling.
    This class allows for the consumption of messages from SQS queues and the publishing of command responses.
    It supports hierarchical topic mapping and can handle both events and commands.
    It is designed to work with AWS credentials and SQS queue configurations provided in the SqsConfig object.
    It also allows for prefetching messages from the SQS queue to improve performance.
    This class is thread-safe and can be used in a multi-threaded environment.
    It is designed to be used with the Sirabus framework for building event-driven applications.
    It provides methods for running the service bus, stopping it, and sending command responses.
    :param SqsConfig config: The SQS configuration object containing AWS credentials and queue settings.
    :param HierarchicalTopicMap topic_map: The topic map to use for resolving topics.
    :param List[IHandleEvents | IHandleCommands] handlers: The list of event and command handlers to register.
    :param Callable message_reader: Function to deserialize messages from SQS.
    :param Callable command_response_writer: Function to serialize command responses for SQS.
    :param int prefetch_count: The number of messages to prefetch from SQS.
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
    It is designed to work with AWS credentials and SQS queue configurations provided in the SqsConfig object.
    It also allows for prefetching messages from the SQS queue to improve performance.
    """

    def __init__(
        self,
        config: SqsConfig,
        topic_map: HierarchicalTopicMap,
        handlers: List[IHandleEvents | IHandleCommands],
        message_reader: Callable[
            [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent]
        ],
        command_response_writer: Callable[[CommandResponse], Tuple[str, bytes]],
        prefetch_count: int = 10,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Create a new instance of the SQS service bus consumer class.

        :param SqsConfig config: The SQS configuration object containing AWS credentials and queue settings.
        :param HierarchicalTopicMap topic_map: The topic map to use for resolving topics.
        :param List[IHandleEvents | IHandleCommands] handlers: The list of event and command handlers to register.
        :param Callable message_reader: Function to deserialize messages from SQS.
        :param Callable command_response_writer: Function to serialize command responses for SQS.
        :param int prefetch_count: The number of messages to prefetch from SQS.
        :param Optional[logging.Logger] logger: Logger instance to use for logging.
        """
        super().__init__(
            message_reader=message_reader,
            topic_map=topic_map,
            handlers=handlers,
            logger=logger or logging.getLogger("SqsServiceBus"),
        )
        self.__config: SqsConfig = config
        self.__subscriptions: Set[str] = set()
        self.__command_response_writer = command_response_writer
        self._topic_map = topic_map
        self._prefetch_count = prefetch_count
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
        self.__queue_name = self._get_consumer_queue_name(self.__topics)
        self.__sqs_thread: Optional[Thread] = None

    async def run(self):
        self._logger.debug("Starting service bus")
        sns_client = self.__config.to_sns_client()
        sqs_client = self.__config.to_sqs_client()
        declared_queue_response = sqs_client.create_queue(QueueName=self.__queue_name)
        queue_url = declared_queue_response["QueueUrl"]
        queue_attributes = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        relationships = self._topic_map.build_parent_child_relationships()
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
        arn = self._topic_map.get_metadata(topic, "arn")
        subscription_response = sns_client.subscribe(
            TopicArn=arn,
            Protocol="sqs",
            Endpoint=queue_url,
        )
        self.__subscriptions.add(subscription_response["SubscriptionArn"])
        self._logger.debug(f"Queue {self.__queue_name} bound to topic {topic}.")

    def _consume_messages(self, queue_url: str):
        """
        Starts consuming messages from the SQS queue.
        :param queue_url: The URL of the SQS queue to consume messages from.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        sqs_client = self.__config.to_sqs_client()
        from botocore.exceptions import EndpointConnectionError
        from urllib3.exceptions import NewConnectionError

        while not self._stopped:
            try:
                response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=self._prefetch_count,
                    WaitTimeSeconds=3,
                )
            except (
                EndpointConnectionError,
                NewConnectionError,
                ConnectionRefusedError,
            ):
                break
            except Exception as e:
                self._logger.exception(
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
                    self._logger.exception(
                        f"Error processing message {message['MessageId']}", exc_info=e
                    )

    async def stop(self):
        self._stopped = True

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
        sqs_client = self.__config.to_sqs_client()
        topic, body = self.__command_response_writer(response)
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

    @staticmethod
    def _get_consumer_queue_name(topics: Set[str]) -> str:
        """
        Returns the queue name for the given topic.
        :param topics: The topics for which to get the queue name.
        :return: The queue name.
        """
        import hashlib

        h = hashlib.sha256(usedforsecurity=False)
        for topic in topics:
            h.update(topic.encode())
        hashed_topics = h.hexdigest()
        return "sqs_" + hashed_topics
