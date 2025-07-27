import logging
from typing import List, Set

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractChannel, ExchangeType

from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class TopographyBuilder:
    def __init__(self, amqp_url: str, topic_map: HierarchicalTopicMap) -> None:
        self.__amqp_url = amqp_url
        self.__topic_map = topic_map

    async def build(self) -> None:
        connection: AbstractRobustConnection = await aio_pika.connect_robust(
            url=self.__amqp_url
        )
        await connection.connect()
        channel: AbstractChannel = await connection.channel()
        await self._build_topography(channel=channel)
        logging.debug("Topography built and consumers registered.")

    async def _build_topography(self, channel: AbstractChannel) -> None:
        exchanges: Set[str] = set()
        bindings: List[str] = []
        alltopics: Set[str] = set(self.__topic_map.get_all_hierarchical_topics())
        for topic in alltopics:
            await channel.declare_exchange(
                name=topic, type=ExchangeType.TOPIC, durable=True
            )
            exchanges.add(topic)
        for key in list(exchanges):
            parts = key.split(".")
            if len(parts) > 1:
                for i in range(1, len(parts)):
                    parent = ".".join(parts[:-i])
                    # Declare the parent exchange if it does not exist
                    if parent not in exchanges:
                        await channel.declare_exchange(
                            name=parent,
                            type=ExchangeType.TOPIC,
                            durable=True,
                        )
                        exchanges.add(parent)
                    # Bind the parent exchange to the child exchange
                    if i > 1:
                        routing_key = f"{parent}.#"
                    else:
                        routing_key = key
                    if routing_key not in bindings:
                        destination = await channel.get_exchange(key)
                        await destination.bind(exchange=parent, routing_key=routing_key)
                        bindings.append(routing_key)
            else:
                # If it's a root topic, ensure it is bound to the amq.topic exchange
                if key not in bindings:
                    destination = await channel.get_exchange(key)
                    bind_response = await destination.bind(
                        exchange="amq.topic", routing_key=f"{key}.#"
                    )
                    logging.debug(f"Bound to amq.topic with response {bind_response}")
                    bindings.append(key)
        await channel.close()
