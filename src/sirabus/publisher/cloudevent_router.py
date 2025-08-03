from typing import Tuple

from aio_pika.abc import (
    AbstractIncomingMessage,
)

from sirabus import TCommand, CommandResponse
from sirabus.publisher.amqp_command_router import AmqpCommandRouter


class CloudEventRouter(AmqpCommandRouter):
    def _create_message(
        self, command: TCommand, response_queue: str
    ) -> Tuple[str, str, str]:
        from sirabus.publisher import create_cloud_command

        return create_cloud_command(
            command=command, topic_map=self._topic_map
        )

    def _read_amqp_response(
        self, response_msg: AbstractIncomingMessage
    ) -> CommandResponse:
        """
        Reads the response message and returns a CommandResponse.
        :param response_msg: The response message received from the command.
        :return: A CommandResponse indicating the success or failure of the command routing.
        """
        try:
            from cloudevents.pydantic import CloudEvent

            cloud_event = CloudEvent.model_validate_json(response_msg.body)
            return CommandResponse.model_validate(cloud_event.data)
        except Exception as e:
            self._logger.exception(f"Error processing response: {e}", exc_info=e)
            return CommandResponse(success=False, message=str(e))
