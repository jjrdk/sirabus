import logging
from typing import Set

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
        await self._declare_exchanges(channel, exchanges)
        relationships = self.__topic_map.build_parent_child_relationships()
        for parent in relationships:
            for child in relationships[parent]:
                # Declare the child exchange if it does not exist
                if child not in exchanges:
                    await self._declare_exchange(
                        topic=child, channel=channel, exchanges=exchanges
                    )
                # Bind the child exchange to the parent exchange
                destination = await channel.get_exchange(child)
                bind_response = await destination.bind(
                    exchange=parent, routing_key=f"{child}.#"
                )
                logging.debug(f"Bound {child} to {parent} with response {bind_response}")
        await channel.close()

    async def _declare_exchanges(self, channel, exchanges):
        all_topics = set(self.__topic_map.get_all_hierarchical_topics())
        for topic in all_topics:
            await self._declare_exchange(
                topic=topic, channel=channel, exchanges=exchanges
            )

    @staticmethod
    async def _declare_exchange(
        topic: str, channel: AbstractChannel, exchanges: Set[str]
    ) -> None:
        await channel.declare_exchange(
            name=topic, type=ExchangeType.TOPIC, durable=True
        )
        exchanges.add(topic)
