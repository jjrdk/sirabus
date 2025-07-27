import inspect
from typing import Dict, Set, Self, Any, List, Iterable

from aett.eventstore import Topic, BaseEvent
from pydantic import BaseModel


class HierarchicalTopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self) -> None:
        self.__topics: Dict[str, type] = {}
        self.__excepted_bases__: Set[type] = {object, BaseModel, BaseEvent}

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

    def _resolve_topics(self, t: type, prefix: str | None = None) -> Iterable[str]:
        topic = t.__topic__ if hasattr(t, "__topic__") else t.__name__
        if t.__base__ and t.__base__ not in self.__excepted_bases__:
            yield from self._resolve_topics(t.__base__, prefix)
        yield topic if prefix is None else f"{prefix}.{topic}"

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
            n = list(self._resolve_topics(t))
            return next(iter(n), None) if n else None
        return None

    def get_all(self) -> List[str]:
        """
        Gets all the topics and their corresponding classes in the map.
        :return: A dictionary of all the topics and their classes.
        """
        return list(self.__topics.keys())

    def get_all_hierarchical_topics(self) -> Iterable[str]:
        """
        Gets all the hierarchical topics in the map.
        :return: A list of all the hierarchical topics.
        """
        for topic in self.__topics.values():
            yield from self._resolve_topics(topic)
