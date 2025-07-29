from typing import Tuple, List

from aett.eventstore import BaseEvent
from cloudevents.pydantic import CloudEvent

from sirabus import IHandleEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.servicebus import ServiceBus
from sirabus.servicebus.inmemory_servicebus import InMemoryServiceBus
from sirabus.message_pump import MessagePump


def _transform_cloudevent_message(
    topic_map: HierarchicalTopicMap, properties: dict, body: bytes
) -> Tuple[dict, BaseEvent]:
    ce = CloudEvent.model_validate_json(body)
    event_type = topic_map.resolve_type(ce.type)
    if event_type is None:
        raise ValueError(f"Event type {ce.type} not found in topic map")
    if event_type and not issubclass(event_type, BaseEvent):
        raise TypeError(f"Event type {event_type} is not a subclass of BaseModel")
    event = event_type.model_validate(ce.data)
    return properties, event


def create_servicebus_for_amqp_cloudevent(
    amqp_url: str,
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents],
    prefetch_count: int = 10,
) -> ServiceBus:
    from sirabus.servicebus.amqp_servicebus import AmqpServiceBus

    return AmqpServiceBus(
        amqp_url=amqp_url,
        topic_map=topic_map,
        handlers=handlers,
        prefetch_count=prefetch_count,
        message_reader=_transform_cloudevent_message,
    )


def create_servicebus_for_memory_cloudevent(
    topic_map: HierarchicalTopicMap,
    handlers: List[IHandleEvents],
    message_pump: MessagePump,
) -> ServiceBus:
    return InMemoryServiceBus(
        topic_map=topic_map,
        handlers=handlers,
        message_reader=_transform_cloudevent_message,
        message_pump=message_pump,
    )
