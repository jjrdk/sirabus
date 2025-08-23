import logging
from typing import Set

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractChannel, ExchangeType

from sirabus.hierarchical_topicmap import HierarchicalTopicMap


class TopographyBuilder:
    """
    Builds the topography for the AMQP service bus by declaring exchanges and binding them according to the
    hierarchical topic map.
    """

    def __init__(self, amqp_url: str, topic_map: HierarchicalTopicMap) -> None:
        """
        Initializes the TopographyBuilder with the AMQP URL and topic map.
        :param amqp_url: The AMQP URL for the service bus.
        :param topic_map: The hierarchical topic map for topic resolution.
        :raises ValueError: If the topic map is not provided.
        :raises TypeError: If the topic map is not an instance of HierarchicalTopicMap.
        :raises Exception: If there is an error during topography building.
        """
        self.__amqp_url = amqp_url
        self.__topic_map = topic_map

    async def build(self) -> None:
        """
        Builds the topography by connecting to the AMQP server, declaring exchanges, and binding them according to the
        hierarchical topic map.
        :raises Exception: If there is an error during the connection or topography building.
        :return: None
        :raises aio_pika.exceptions.AMQPConnectionError: If the connection to the AMQP server fails.
        :raises aio_pika.exceptions.ChannelClosed: If the channel is closed unexpectedly.
        :raises aio_pika.exceptions.ExchangeDeclareError: If there is an error declaring an exchange.
        :raises aio_pika.exceptions.ExchangeBindError: If there is an error binding an exchange.
        :raises aio_pika.exceptions.AMQPError: For any other AMQP-related errors
        """
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
                logging.debug(
                    f"Bound {child} to {parent} with response {bind_response}"
                )
        await channel.close()

    async def _declare_exchanges(self, channel, exchanges):
        all_topics = set(self.__topic_map.get_all())
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
