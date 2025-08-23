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
    time: str = Field(description="ISO 8601 timestamp of the event")
    source: str = Field(description="Source of the event")
    subject: str = Field(description="Subject of the event")
    type: str = Field(description="Type of the event")
    reply_to: Optional[str] = Field(
        description="The optional reply-to address for the message", default=None
    )


def write_cloudevent_message(
    topic_map: HierarchicalTopicMap, properties: dict, body: bytes
) -> Tuple[dict, BaseEvent]:
    """
    Reads a CloudEvent message from the body and validates it against the topic map.
    :param topic_map: The hierarchical topic map to validate the event type.
    :param properties: Additional properties to include in the event.
    :param body: The body of the CloudEvent message in JSON format.
    :return: A tuple containing the properties and the validated event.
    :raises ValueError: If the event type is not found in the topic map.
    :raises TypeError: If the event type is not a subclass of BaseModel.
    """
    ce = CloudEvent.model_validate_json(body)
    event_type = topic_map.get(ce.type)
    if event_type is None:
        raise ValueError(f"Event type {ce.type} not found in topic map")
    if event_type and not issubclass(event_type, BaseModel):
        raise TypeError(f"Event type {event_type} is not a subclass of BaseModel")
    event = event_type.model_validate(ce.data)
    return properties, event


def create_event[TEvent: BaseEvent](
    event: TEvent, topic_map: HierarchicalTopicMap
) -> Tuple[str, str, str]:
    """
    Creates a CloudEvent from the given event and topic map.
    :param event: The event to create a CloudEvent for.
    :param topic_map: The hierarchical topic map to find the topic for the event type.
    :return: A tuple containing the topic, hierarchical topic, and the CloudEvent JSON string.
    :raises ValueError: If the topic for the event type is not found in the hierarchical_topic_map.
    :raises TypeError: If the event type is not a subclass of BaseModel.
    """
    event_type = type(event)
    topic = Topic.get(event_type)
    hierarchical_topic = topic_map.get_from_type(event_type)

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


def create_command[TCommand: BaseCommand](
    command: TCommand, topic_map: HierarchicalTopicMap
) -> Tuple[str, str, str]:
    """
    Creates a CloudEvent from the given command and topic map.
    :param command: The command to create a CloudEvent for.
    :param topic_map: The hierarchical topic map to find the topic for the command type.
    :return: A tuple containing the topic, hierarchical topic, and the CloudEvent JSON string.
    :raises ValueError: If the topic for the command type is not found in the hierarchical_topic_map.
    :raises TypeError: If the command type is not a subclass of BaseModel.
    """
    command_type = type(command)
    topic = Topic.get(command_type)
    hierarchical_topic = topic_map.get_from_type(command_type)

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
    """
    Creates a CloudEvent from the given command response.
    :param command_response: The command response to create a CloudEvent for.
    :return: A tuple containing the topic and the CloudEvent JSON string.
    :raises ValueError: If the command response type is not found in the Topic enum.
    :raises TypeError: If the command response type is not a subclass of BaseModel.
    """
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
    """
    Reads a command response from the CloudEvent message.
    :param headers: The headers of the CloudEvent message.
    :param response_msg: The body of the CloudEvent message in JSON format.
    :return: A CommandResponse if the message is a valid command response, otherwise None.
    :raises ValueError: If the response message cannot be processed as a CloudEvent.
    :raises TypeError: If the response message type is not a subclass of BaseModel.
    """
    try:
        cloud_event = CloudEvent.model_validate_json(response_msg)
        if cloud_event.type == Topic.get(CommandResponse):
            return CommandResponse.model_validate(cloud_event.data)
        return None
    except Exception as e:
        raise ValueError(f"Error processing response: {e}")
