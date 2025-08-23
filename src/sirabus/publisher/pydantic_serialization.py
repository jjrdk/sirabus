from typing import Tuple

from aett.eventstore import Topic, BaseEvent, BaseCommand
from pydantic import BaseModel

from sirabus import CommandResponse
from sirabus.hierarchical_topicmap import HierarchicalTopicMap


def create_event[TEvent: BaseEvent](
    event: TEvent, topic_map: HierarchicalTopicMap
) -> Tuple[str, str, str]:
    """
    Create an event message for publishing.
    :param event: The event to publish.
    :param topic_map: The hierarchical topic map to use for topic resolution.
    :return: A tuple containing the topic, hierarchical topic, and JSON representation of the event.
    """
    event_type = type(event)
    topic = Topic.get(event_type)
    hierarchical_topic = topic_map.get_from_type(event_type)

    if not hierarchical_topic:
        raise ValueError(
            f"Topic for event type {event_type} not found in hierarchical_topic map."
        )
    j = event.model_dump_json()
    return topic, hierarchical_topic, j


def read_event_message(
    topic_map: HierarchicalTopicMap, properties: dict, body: bytes
) -> Tuple[dict, BaseEvent]:
    """
    Read an event message from the message body and properties.
    :param topic_map: The hierarchical topic map to use for topic resolution.
    :param properties: The properties of the message, including the topic.
    :param body: The message body containing the event data.
    :return: A tuple containing the properties and the validated event.
    :raises ValueError: If the topic is not found in the topic map.
    :raises TypeError: If the event type is not a subclass of BaseModel.
    """
    topic = properties["topic"]
    event_type = topic_map.get(topic)
    if event_type is None:
        raise ValueError(f"Event type {topic} not found in topic map")
    if event_type and not issubclass(event_type, BaseModel):
        raise TypeError(f"Event type {event_type} is not a subclass of BaseModel")
    event = event_type.model_validate_json(body)
    return properties, event


def create_command[TCommand: BaseCommand](
    command: TCommand, topic_map: HierarchicalTopicMap
) -> Tuple[str, str, str]:
    """
    Create a command message for publishing.
    :param command: The command to publish.
    :param topic_map: The hierarchical topic map to use for topic resolution.
    :return: A tuple containing the topic, hierarchical topic, and JSON representation of the command
    """
    command_type = type(command)
    topic = Topic.get(command_type)
    hierarchical_topic = topic_map.get_from_type(command_type)

    if not hierarchical_topic:
        raise ValueError(
            f"Topic for event type {command_type} not found in hierarchical_topic map."
        )
    j = command.model_dump_json()
    return topic, hierarchical_topic, j


def create_command_response(
    command_response: CommandResponse,
) -> Tuple[str, bytes]:
    """
    Create a command response message for publishing.
    :param command_response: The command response to publish.
    :return: A tuple containing the topic and JSON representation of the command response.
    """
    topic = Topic.get(type(command_response))
    j = command_response.model_dump_json().encode()
    return topic, j


def read_command_response(
    headers: dict,
    response_msg: bytes,
) -> CommandResponse | None:
    """
    Read a command response message from the response body.
    :param headers: The headers of the message, including the topic.
    :param response_msg: The message body containing the command response data.
    :return: A CommandResponse object if the message is valid, otherwise None.
    """
    try:
        response = CommandResponse.model_validate_json(response_msg)
        return response if response.message != "" else None
    except Exception as e:
        raise ValueError(f"Error processing response: {e}")
