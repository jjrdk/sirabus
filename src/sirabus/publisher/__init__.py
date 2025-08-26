import abc
from typing import Callable, Tuple

from aett.eventstore import BaseEvent

from sirabus import EndpointConfiguration, HierarchicalTopicMap


class PublisherConfiguration(EndpointConfiguration, abc.ABC):
    def __init__(
        self, event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str]]
    ):
        super().__init__()
        self._event_writer = event_writer

    def write_event(self, event: BaseEvent) -> Tuple[str, str]:
        return self._event_writer(event, self.get_topic_map())
