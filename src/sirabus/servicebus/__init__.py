import abc
import asyncio
import logging
from typing import Tuple, Callable, List

from aett.eventstore import BaseEvent
from aett.eventstore.base_command import BaseCommand

from sirabus import IHandleEvents, IHandleCommands, CommandResponse, get_type_param
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class ServiceBus(abc.ABC):
    def __init__(
            self,
            topic_map: HierarchicalTopicMap,
            message_reader: Callable[
                [HierarchicalTopicMap, dict, bytes], Tuple[dict, BaseEvent | BaseCommand]
            ],
            handlers: List[IHandleEvents | IHandleCommands],
            logger: logging.Logger,
    ) -> None:
        self._logger = logger
        self._topic_map = topic_map
        self._message_reader = message_reader
        self._handlers = handlers

    @abc.abstractmethod
    async def run(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def stop(self):
        raise NotImplementedError()

    async def handle_message(
            self,
            headers: dict,
            body: bytes,
            correlation_id: str | None,
            reply_to: str | None,
    ) -> None:
        headers, event = self._message_reader(self._topic_map, headers, body)
        if isinstance(event, BaseEvent):
            await self.handle_event(event, headers)
        elif isinstance(event, BaseCommand):
            from aett.eventstore import Topic

            command_handler = next(
                (
                    h
                    for h in self._handlers
                    if isinstance(h, IHandleCommands)
                    and Topic.get(type(event)) == Topic.get(get_type_param(h))
                ),
                None,
            )
            if not command_handler:
                if not reply_to:
                    self._logger.error(
                        f"No command handler found for command {type(event)} with correlation ID {correlation_id} "
                        f"and no reply_to field provided."
                    )
                    return
                await self.send_command_response(
                    response=CommandResponse(success=False, message="unknown command"),
                    correlation_id=correlation_id,
                    reply_to=reply_to,
                )
                return
            response = await command_handler.handle(command=event, headers=headers)
            if not reply_to:
                self._logger.error(
                    f"Reply to field is empty for command {type(event)} with correlation ID {correlation_id}."
                )
                return
            await self.send_command_response(
                response=response, correlation_id=correlation_id, reply_to=reply_to
            )
        elif isinstance(event, CommandResponse):
            pass
        else:
            raise TypeError(f"Unexpected message type: {type(event)}")

    async def handle_event(self, event: BaseEvent, headers: dict) -> None:
        await asyncio.gather(
            *[
                h.handle(event=event, headers=headers)
                for h in self._handlers
                if isinstance(h, IHandleEvents)
                and isinstance(event, get_type_param(h))
            ],
            return_exceptions=True,
        )

    @abc.abstractmethod
    async def send_command_response(
            self, response: CommandResponse, correlation_id: str | None, reply_to: str
    ) -> None:
        pass


def create_servicebus_for_amqp_pydantic(
        amqp_url: str,
        topic_map: HierarchicalTopicMap,
        event_handlers: List[IHandleEvents | IHandleCommands],
        logger=None,
        prefetch_count=10,
):
    from sirabus.servicebus.amqp_servicebus import AmqpServiceBus
    from sirabus.publisher.pydantic_serialization import read_event_message
    from sirabus.publisher.pydantic_serialization import create_command_response

    return AmqpServiceBus(
        amqp_url=amqp_url,
        topic_map=topic_map,
        handlers=event_handlers,
        message_reader=read_event_message,
        command_response_writer=create_command_response,
        logger=logger,
        prefetch_count=prefetch_count,
    )
