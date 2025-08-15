import asyncio
import logging
from threading import Thread
from uuid import uuid4
from typing import Dict, Tuple, Optional, Callable

from aett.eventstore.base_command import BaseCommand
from sirabus import CommandResponse, IRouteCommands, SqsConfig
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class SqsCommandRouter(IRouteCommands):
    def __init__(
        self,
        config: SqsConfig,
        topic_map: HierarchicalTopicMap,
        message_writer: Callable[
            [BaseCommand, HierarchicalTopicMap], Tuple[str, str, str]
        ],
        response_reader: Callable[[dict, bytes], CommandResponse | None],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initializes the SqsCommandRouter.
        :param config: The SQS configuration.
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
        self.__config = config
        self.__topic_map = topic_map
        self.__logger = logger or logging.getLogger("SqsCommandRouter")

    async def route[TCommand: BaseCommand](
        self, command: TCommand
    ) -> asyncio.Future[CommandResponse]:
        loop = asyncio.get_event_loop()
        try:
            _, hierarchical_topic, j = self.__message_writer(command, self.__topic_map)
        except ValueError:
            future = loop.create_future()
            future.set_result(CommandResponse(success=False, message="unknown command"))
            return future
        sqs_client = self.__config.to_sqs_client()
        declared_queue_response = sqs_client.create_queue(
            QueueName=f"sqs_{str(uuid4())}"
        )
        queue_url = declared_queue_response["QueueUrl"]
        consume_thread = Thread(target=self._consume_queue, args=(queue_url,))
        consume_thread.start()
        sns_client = self.__config.to_sns_client()
        import json

        metadata = self.__topic_map.get_metadata(hierarchical_topic, "arn")
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
                    "StringValue": self.__topic_map.get_from_type(type(command)),
                    "DataType": "String",
                },
                "reply_to": {
                    "StringValue": queue_url,
                    "DataType": "String",
                },
            },
        )
        message_id = response["MessageId"]
        self.__logger.debug(f"Published {hierarchical_topic}")
        future = loop.create_future()
        self.__inflight[message_id] = (future, consume_thread)
        return future

    def _consume_queue(self, queue_url: str) -> None:
        import time

        sqs_client = self.__config.to_sqs_client()
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
                self.__logger.exception(
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
                response = self.__response_reader(message_attributes, body.encode())
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
                        response = sqs_client.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )
                except Exception as e:
                    self.__logger.exception(
                        f"Error deleting message {message['MessageId']}", exc_info=e
                    )
        _ = sqs_client.delete_queue(QueueUrl=queue_url)
