import datetime
import uuid
from typing import Optional, Tuple

from aett.eventstore import Topic, BaseEvent, BaseCommand
from cloudevents.pydantic import CloudEvent
from pydantic import BaseModel, Field

from sirabus import CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class CloudEventAttributes(BaseModel):
    id: str = Field(default=str(uuid.uuid4()))
    specversion: str = Field(default="1.0")
    datacontenttype: str = Field(default="application/json")
    time: str = Field()
    source: str = Field()
    subject: str = Field()
    type: str = Field()
    reply_to: Optional[str] = Field(default=None)


def create_event[TEvent:BaseEvent](
        event: TEvent, topic_map: HierarchicalTopicMap
) -> Tuple[str, str, str]:
    event_type = type(event)
    topic = Topic.get(event_type)
    hierarchical_topic = topic_map.get_hierarchical_topic(event_type)

    if not hierarchical_topic:
        raise ValueError(
            f"Topic for event type {event_type} not found in hierarchical_topic map."
        )
    a = CloudEventAttributes(
        id=str(uuid.uuid4()),
        specversion="1.0",
        datacontenttype="application/json",
        time=event.timestamp.isoformat(),
        source=event.source,
        subject=topic,
        type=hierarchical_topic or topic,
    )
    ce = CloudEvent.create(
        attributes=a.model_dump(exclude_none=True),
        data=event.model_dump(mode="json"),
    )
    j = ce.model_dump_json()
    return topic, hierarchical_topic, j


def create_command[TCommand:BaseCommand](
        command: TCommand, topic_map: HierarchicalTopicMap
) -> Tuple[str, str, str]:
    command_type = type(command)
    topic = Topic.get(command_type)
    hierarchical_topic = topic_map.get_hierarchical_topic(command_type)

    if not hierarchical_topic:
        raise ValueError(
            f"Topic for event type {command_type} not found in hierarchical_topic map."
        )
    a = CloudEventAttributes(
        id=str(uuid.uuid4()),
        specversion="1.0",
        datacontenttype="application/json",
        time=command.timestamp.isoformat(),
        source=command.aggregate_id,
        subject=topic,
        type=hierarchical_topic or topic,
    )
    ce = CloudEvent.create(
        attributes=a.model_dump(exclude_none=True),
        data=command.model_dump(mode="json"),
    )
    j = ce.model_dump_json()
    return topic, hierarchical_topic, j


def create_command_response(
        command_response: CommandResponse,
) -> Tuple[str, bytes]:
    topic = Topic.get(type(command_response))
    a = CloudEventAttributes(
        id=str(uuid.uuid4()),
        specversion="1.0",
        datacontenttype="application/json",
        time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        source="sirabus",
        subject=topic,
        type=topic,
    )
    ce = CloudEvent.create(
        attributes=a.model_dump(exclude_none=True),
        data=command_response.model_dump(mode="json"),
    )
    j = ce.model_dump_json().encode()
    return topic, j


def read_command_response(
        headers: dict,
        response_msg: bytes,
) -> CommandResponse | None:
    try:
        cloud_event = CloudEvent.model_validate_json(response_msg)
        if cloud_event.type == Topic.get(CommandResponse):
            return CommandResponse.model_validate(cloud_event.data)
        return None
    except Exception as e:
        raise ValueError(f"Error processing response: {e}")
