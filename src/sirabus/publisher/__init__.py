import uuid
from typing import Tuple

from aett.eventstore import Topic, BaseEvent
from cloudevents.pydantic import CloudEvent
from pydantic import BaseModel, Field

from sirabus import TEvent
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.publisher.cloudevent_publisher import (
    create_publisher_for_amqp_cloudevent,
    create_publisher_for_memory_cloudevent,
)


def create_cloud_event(
    event: TEvent, topic_map: HierarchicalTopicMap, logger
) -> Tuple[str, str, str]:
    if not isinstance(event, BaseEvent):
        logger.exception(f"{event} is not an instance of BaseEvent", exc_info=True)
        raise TypeError(f"Expected event of type {type(TEvent)}, got {type(event)}")
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
    ce = CloudEvent(
        attributes=a.model_dump(),
        data=event.model_dump(mode="json"),
    )
    j = ce.model_dump_json()
    return topic, hierarchical_topic, j


class CloudEventAttributes(BaseModel):
    id: str = Field(default=str(uuid.uuid4()))
    specversion: str = Field(default="1.0")
    datacontenttype: str = Field(default="application/json")
    time: str = Field()
    source: str = Field()
    subject: str = Field()
    type: str = Field()
