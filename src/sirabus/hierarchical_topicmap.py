import inspect
from typing import Dict, List, Set, Self, Any

from aett.eventstore import Topic, BaseEvent
from aett.eventstore.base_command import BaseCommand
from pydantic import BaseModel

from sirabus import CommandResponse


class HierarchicalTopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self) -> None:
        self.__metadata: Dict[str, Dict[str, Any]] = {}
        self._topics: Dict[str, type] = {}
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

    def set_metadata(self, topic: str, key: str, value: Any) -> Self:
        """
        Sets metadata for the given topic.
        :param topic: The topic to set metadata for.
        :param key: The key of the metadata.
        :param value: The value of the metadata.
        """
        if topic not in self.__metadata:
            self.__metadata[topic] = {}
        self.__metadata[topic][key] = value
        return self

    def get_metadata(self, topic: str, key: str) -> Any:
        """
        Gets metadata for the given topic.
        :param topic: The topic to get metadata for.
        :param key: The key of the metadata.
        :return: The value of the metadata.
        """
        return self.__metadata.get(topic, {}).get(key, None)

    def add(self, topic: str, cls: type) -> Self:
        """
        Adds the topic and class to the map.
        :param topic: The topic of the event.
        :param cls: The class of the event.
        """
        self._topics[topic] = cls
        return self

    def register(self, instance: Any) -> Self:
        t = instance if isinstance(instance, type) else type(instance)
        hierarchical_topic = self._get_hierarchical_topic(t)
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

    def get(self, topic: str) -> type | None:
        """
        Gets the class of the event given the topic.
        :param topic: The topic of the event.
        :return: The class of the event.
        """
        return self._topics.get(topic, None)

    def get_from_type(self, t: type) -> str | None:
        """
        Gets the topic of the event given the class.
        :param t: The class of the event.
        :return: The topic of the event.
        """
        for topic, cls in self._topics.items():
            if cls is t:
                return topic
        return None

    def get_all(self) -> List[str]:
        """
        Gets all the topics and their corresponding classes in the map.
        :return: A dictionary of all the topics and their classes.
        """
        return list(self._topics.keys())

    def _get_hierarchical_topic(self, instance: type | None) -> str | None:
        """
        Gets the topic of the event given the class.
        :param instance: The class of the event.
        :return: The topic of the event.
        """
        if instance is None:
            return None
        if instance in self._topics.values():
            return next(topic for topic, cls in self._topics.items() if cls is instance)
        return self._resolve_topics(instance)

    def build_parent_child_relationships(self) -> Dict[str, Set[str]]:
        """
        Builds a list of parent-child relationships for the given topic.
        :return: A list of parent-child relationships.
        """

        relationships: Dict[str, Set[str]] = {}

        def visit(cls: type) -> None:
            for base in cls.__bases__:
                if base not in self.__excepted_bases__:
                    parent_type = self.get(self.get_from_type(base))
                    if not parent_type:
                        raise RuntimeError(
                            f"Base class '{base.__name__}' for '{cls.__name__}' not found in the topic map."
                        )
                    parent = self.get_from_type(parent_type)
                    if not parent:
                        raise RuntimeError(
                            f"Parent topic for class '{cls.__name__}' not found in the topic map."
                        )
                    child_type = self.get(self.get_from_type(cls))
                    if not child_type:
                        raise RuntimeError(
                            f"Child class '{cls.__name__}' not found in the topic map."
                        )
                    child = self.get_from_type(child_type)
                    if not child:
                        raise RuntimeError(
                            f"Child topic for class '{cls.__name__}' not found in the topic map."
                        )
                    relationships.setdefault(parent, set()).add(child)
                    visit(base)

        for instance in self._topics.values():
            if any(t for t in instance.__bases__ if t in self.__excepted_bases__):
                relationships.setdefault("amq.topic", set()).add(
                    self.get_from_type(instance)
                )
            visit(instance)
        return relationships
