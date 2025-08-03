import inspect
from typing import Dict, Set, Self, Any, Iterable

from aett.eventstore import Topic, BaseEvent
from aett.eventstore.base_command import BaseCommand
from pydantic import BaseModel

from sirabus import CommandResponse


class HierarchicalTopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self) -> None:
        self.__topics: Dict[str, type] = {}
        self.__excepted_bases__: Set[type] = {object, BaseModel, BaseEvent, BaseCommand}
        self.add(Topic.get(CommandResponse), CommandResponse)

    def add(self, topic: str, cls: type) -> Self:
        """
        Adds the topic and class to the map.
        :param topic: The topic of the event.
        :param cls: The class of the event.
        """
        self.__topics[topic] = cls
        return self

    def except_base(self, t: type) -> None:
        """
        Exclude the base class from the topic hierarchy.
        :param t: The class to exclude.
        """
        if not isinstance(t, type):
            raise TypeError("Expected a class type")
        if t not in self.__excepted_bases__:
            self.__excepted_bases__.add(t)

    def register(self, instance: Any) -> Self:
        t = instance if isinstance(instance, type) else type(instance)
        topic = Topic.get(t)
        if topic not in self.__topics:
            self.add(topic, t)

        return self

    def _resolve_topics(self, t: type, suffix: str | None = None) -> str:
        topic = t.__topic__ if hasattr(t, "__topic__") else t.__name__
        # yield topic if suffix is None else f"{topic}.{suffix}"
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

    def resolve_type(self, topic: str) -> type | None:
        """
        Gets the class of the event given the topic.
        :param topic: The topic of the event.
        :return: The class of the event.
        """
        return self.__topics.get(topic, None)

    def get_hierarchical_topic(self, instance: type) -> str | None:
        """
        Gets the topic of the event given the class.
        :param instance: The class of the event.
        :return: The topic of the event.
        """
        t = instance if isinstance(instance, type) else type(instance)
        if t in self.__topics.values():
            n = self._resolve_topics(t)
            return n
        return None

    def get_all_hierarchical_topics(self) -> Iterable[str]:
        """
        Gets all the hierarchical topics in the map.
        :return: A list of all the hierarchical topics.
        """
        for topic in self.__topics.values():
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
                    parent_type = self.resolve_type(Topic.get(base))
                    if not parent_type:
                        raise RuntimeError(
                            f"Base class '{base.__name__}' for '{cls.__name__}' not found in the topic map."
                        )
                    parent = self.get_hierarchical_topic(parent_type)
                    if not parent:
                        raise RuntimeError(
                            f"Parent topic for class '{cls.__name__}' not found in the topic map."
                        )
                    child_type = self.resolve_type(Topic.get(cls))
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

        for topic in self.__topics.values():
            if any(t for t in topic.__bases__ if t in self.__excepted_bases__):
                relationships.setdefault("amq.topic", set()).add(Topic.get(topic))
            visit(topic)
        return relationships
