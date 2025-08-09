import logging
from typing import Tuple, List

from aett.eventstore import BaseEvent
from cloudevents.pydantic import CloudEvent
from pydantic import BaseModel

from sirabus import IHandleEvents, IHandleCommands
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus
from sirabus.servicebus.inmemory_servicebus import InMemoryServiceBus
from sirabus.message_pump import MessagePump


def _transform_cloudevent_message(
    topic_map: HierarchicalTopicMap, properties: dict, body: bytes
) -> Tuple[dict, BaseEvent]:
    ce = CloudEvent.model_validate_json(body)
    event_type = topic_map.get(ce.type)
    if event_type is None:
        raise ValueError(f"Event type {ce.type} not found in topic map")
    if event_type and not issubclass(event_type, BaseModel):
        raise TypeError(f"Event type {event_type} is not a subclass of BaseModel")
    event = event_type.model_validate(ce.data)
    return properties, event


def create_servicebus_for_amqp_cloudevent(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    prefetch_count: int = 10,
) -> ServiceBus:
    from sirabus.servicebus.amqp_servicebus import AmqpServiceBus

    from sirabus.publisher.cloudevent_serialization import create_command_response

    return AmqpServiceBus(
        amqp_url=amqp_url,
        topic_map=topic_map,
        handlers=handlers,
        prefetch_count=prefetch_count,
        message_reader=_transform_cloudevent_message,
        command_response_writer=create_command_response,
    )


def create_servicebus_for_memory_cloudevent(
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents | IHandleCommands],
    message_pump: MessagePump,
) -> ServiceBus:
    from sirabus.publisher.cloudevent_serialization import create_command_response

    return InMemoryServiceBus(
        topic_map=topic_map,
        handlers=handlers,
        message_reader=_transform_cloudevent_message,
        response_writer=create_command_response,
        message_pump=message_pump,
        logger=logging.getLogger("InMemoryServiceBus"),
    )
