import inspect
from typing import Dict, Set, Self, Any, Iterable

from aett.eventstore import Topic, BaseEvent, TopicMap
from aett.eventstore.base_command import BaseCommand
from pydantic import BaseModel

from sirabus import CommandResponse


class HierarchicalTopicMap(TopicMap):
    """
    Represents a map of topics to event classes.
    """

    def __init__(self) -> None:
        super().__init__()
        self.__excepted_bases__: Set[type] = {object, BaseModel, BaseEvent, BaseCommand}
        self.add(Topic.get(CommandResponse), CommandResponse)

    def except_base(self, t: type) -> None:
        """
        Exclude the base class from the topic hierarchy.
        :param t: The class to exclude.
        """
        if not isinstance(t, type):
            raise TypeError(f"except_base expects a type, got {type(t).__name__}")
        if t not in self.__excepted_bases__:
            self.__excepted_bases__.add(t)

    def register(self, instance: Any) -> Self:
        t = instance if isinstance(instance, type) else type(instance)
        topic = Topic.get(t)
        if topic not in self.get_all():
            self.add(topic, t)
        hierarchical_topic = self.get_hierarchical_topic(t)
        if hierarchical_topic is not None:
            self.add(hierarchical_topic, t)

        return self

    def _resolve_topics(self, t: type, suffix: str | None = None) -> str:
        topic = t.__topic__ if hasattr(t, "__topic__") else t.__name__
        if any(tb for tb in t.__bases__ if tb not in self.__excepted_bases__):
            tbase = self._resolve_topics(t.__bases__[0], suffix)
            topic = (
                f"{tbase}.{topic}" if suffix is None else f"{tbase}.{topic}.{suffix}"
            )
            return topic
        return topic

    def register_module(self, module: object) -> Self:
        """
        Registers all the classes in the module.
        """
        for _, o in inspect.getmembers(module, inspect.isclass):
            if inspect.isclass(o):
                self.register(o)
            if inspect.ismodule(o):
                self.register_module(o)
        return self

    def get_hierarchical_topic(self, instance: type) -> str | None:
        """
        Gets the topic of the event given the class.
        :param instance: The class of the event.
        :return: The topic of the event.
        """
        if instance in self.get_all_types():
            n = self._resolve_topics(instance)
            return n
        return None

    def get_all_hierarchical_topics(self) -> Iterable[str]:
        """
        Gets all the hierarchical topics in the map.
        :return: A list of all the hierarchical topics.
        """
        for topic in self.get_all_types():
            yield self._resolve_topics(topic)

    def build_parent_child_relationships(self) -> Dict[str, Set[str]]:
        """
        Builds a list of parent-child relationships for the given topic.
        :return: A list of parent-child relationships.
        """

        relationships: Dict[str, Set[str]] = {}

        def visit(cls: type) -> None:
            for base in cls.__bases__:
                if base not in self.__excepted_bases__:
                    parent_type = self.get(Topic.get(base))
                    if not parent_type:
                        raise RuntimeError(
                            f"Base class '{base.__name__}' for '{cls.__name__}' not found in the topic map."
                        )
                    parent = self.get_hierarchical_topic(parent_type)
                    if not parent:
                        raise RuntimeError(
                            f"Parent topic for class '{cls.__name__}' not found in the topic map."
                        )
                    child_type = self.get(Topic.get(cls))
                    if not child_type:
                        raise RuntimeError(
                            f"Child class '{cls.__name__}' not found in the topic map."
                        )
                    child = self.get_hierarchical_topic(child_type)
                    if not child:
                        raise RuntimeError(
                            f"Child topic for class '{cls.__name__}' not found in the topic map."
                        )
                    relationships.setdefault(parent, set()).add(child)
                    visit(base)

        for topic in self.get_all_types():
            if any(t for t in topic.__bases__ if t in self.__excepted_bases__):
                relationships.setdefault("amq.topic", set()).add(Topic.get(topic))
            visit(topic)
        return relationships
