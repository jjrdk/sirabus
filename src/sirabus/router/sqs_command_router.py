import asyncio
from threading import Thread
from typing import Dict, Tuple, Optional, Callable, Self
from uuid import uuid4

from aett.eventstore.base_command import BaseCommand

from sirabus import CommandResponse, IRouteCommands
from sirabus.shared.sqs_config import SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.router import RouterConfiguration


class SqsRouterConfiguration(RouterConfiguration):
    """Configuration for SQS Command Router."""

    def __init__(
        self,
        message_writer: Callable[[BaseCommand, HierarchicalTopicMap], Tuple[str, str]],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
    ) -> None:
        """
        Initializes the SqsRouterConfiguration with the necessary parameters.
        :param Callable message_writer: A callable that formats the command into a message.
        :param Callable response_reader: A callable that reads the response from the message.
        :raises ValueError: If the amqp_url is empty.
        """
        super().__init__(message_writer=message_writer, response_reader=response_reader)
        self._sqs_config: Optional[SqsConfig] = None

    def get_sqs_config(self) -> SqsConfig:
        """
        Gets the SQS configuration.
        :return: The SQS configuration.
        :raises ValueError: If the sqs_config is not set.
        """
        if not self._sqs_config:
            raise ValueError("sqs_config has not been set.")
        return self._sqs_config

    def with_sqs_config(self, config: SqsConfig) -> Self:
        """
        Sets the SQS configuration.
        :param SqsConfig config: The SQS configuration to set.
        :return: The SqsRouterConfiguration instance.
        :raises ValueError: If the sqs_config is None.
        """
        if not config:
            raise ValueError("config cannot be None.")
        self._sqs_config = config
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import (
            write_command,
            read_command_response,
        )

        return SqsRouterConfiguration(
            message_writer=write_command, response_reader=read_command_response
        )

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import (
            write_command,
            read_command_response,
        )

        return SqsRouterConfiguration(
            message_writer=write_command, response_reader=read_command_response
        )


class SqsCommandRouter(IRouteCommands):
    def __init__(self, configuration: SqsRouterConfiguration) -> None:
        """
        Initializes the SqsCommandRouter.
        :param SqsRouterConfiguration configuration: The SQS router configuration.
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
        sqs_client = self._configuration.get_sqs_config().to_sqs_client()
        declared_queue_response = sqs_client.create_queue(
            QueueName=f"sqs_{str(uuid4())}"
        )
        queue_url = declared_queue_response["QueueUrl"]
        consume_thread = Thread(target=self._consume_queue, args=(queue_url,))
        consume_thread.start()
        sns_client = self._configuration.get_sqs_config().to_sns_client()
        import json

        metadata = self._configuration.get_topic_map().get_metadata(
            hierarchical_topic, "arn"
        )
        response = sns_client.publish(
            TopicArn=metadata,
            Message=json.dumps({"default": j}),
            Subject=hierarchical_topic,
            MessageStructure="json",
            MessageAttributes={
                "correlation_id": {
                    "StringValue": command.correlation_id,
                    "DataType": "String",
                },
                "topic": {
                    "StringValue": self._configuration.get_topic_map().get_from_type(
                        type(command)
                    ),
                    "DataType": "String",
                },
                "reply_to": {
                    "StringValue": queue_url,
                    "DataType": "String",
                },
            },
        )
        message_id = response["MessageId"]
        self._configuration.get_logger().debug(f"Published {hierarchical_topic}")
        future = loop.create_future()
        self.__inflight[message_id] = (future, consume_thread)
        return future

    def _consume_queue(self, queue_url: str) -> None:
        import time

        sqs_client = self._configuration.get_sqs_config().to_sqs_client()
        response_received = False
        while not response_received:
            try:
                response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=3,
                )
            except Exception as e:
                self._configuration.get_logger().exception(
                    "Error receiving messages from SQS queue", exc_info=e
                )
                time.sleep(1)
                continue

            messages = response.get("Messages", [])
            if not messages:
                time.sleep(0.1)
                continue
            for message in messages:
                body = message.get("Body", None)
                message_attributes: Dict[str, str] = {}
                for key, value in message.get("MessageAttributes", {}).items():
                    if value.get("StringValue", None) is not None:
                        message_attributes[key] = value.get("StringValue", None)
                response = self._configuration.read_response(
                    message_attributes, body.encode()
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
                        _ = sqs_client.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )
                except Exception as e:
                    self._configuration.get_logger().exception(
                        f"Error deleting message {message['MessageId']}", exc_info=e
                    )
        _ = sqs_client.delete_queue(QueueUrl=queue_url)
