from typing import Callable, Tuple, Optional, Self

from aett.eventstore import BaseEvent
from aio_pika import connect_robust, Message

from sirabus import IPublishEvents
from sirabus.hierarchical_topicmap import HierarchicalTopicMap
from sirabus.publisher import PublisherConfiguration


class AmqpPublisherConfiguration(PublisherConfiguration):
    def __init__(
        self, event_writer: Callable[[BaseEvent, HierarchicalTopicMap], Tuple[str, str]]
    ):
        """
        Initializes the AMQP Publisher Configuration with the necessary parameters.
        :param event_writer: A callable that formats the event into a message.
        """
        super().__init__(event_writer=event_writer)
        self._amqp_url: Optional[str] = None

    def get_amqp_url(self) -> str:
        if not self._amqp_url:
            raise ValueError("amqp_url has not been set.")
        return self._amqp_url

    def with_amqp_url(self, amqp_url: str) -> Self:
        if not amqp_url:
            raise ValueError("amqp_url cannot be None or empty.")
        self._amqp_url = amqp_url
        return self

    @staticmethod
    def default():
        from sirabus.serialization.pydantic_serialization import write_event

        return AmqpPublisherConfiguration(event_writer=write_event)

    @staticmethod
    def for_cloud_event():
        from sirabus.serialization.cloudevent_serialization import write_event

        return AmqpPublisherConfiguration(event_writer=write_event)


class AmqpPublisher(IPublishEvents):
    """
    Publishes events over AMQP.
    """

    def __init__(
        self,
        configuration: AmqpPublisherConfiguration,
    ) -> None:
        """
        Initializes the AMQP Publisher with the necessary parameters.
        :param configuration: The publisher configuration.
        """
        self._configuration = configuration

    async def publish[TEvent: BaseEvent](self, event: TEvent) -> None:
        """
        Publishes the event to the configured topic.
        :param event: The event to publish.
        """

        hierarchical_topic, j = self._configuration.write_event(event)

        connection = await connect_robust(
            url=self._configuration.get_amqp_url(),
            ssl=(self._configuration.get_ssl_config() is not None),
            ssl_context=self._configuration.get_ssl_config(),
        )
        channel = await connection.channel()
        exchange = await channel.get_exchange(name="amq.topic", ensure=False)
        self._configuration.get_logger().debug("Channel opened for publishing event.")
        response = await exchange.publish(
            message=Message(body=j.encode(), headers={"topic": hierarchical_topic}),
            routing_key=hierarchical_topic,
        )
        self._configuration.get_logger().debug(f"Published {response}")
        await channel.close()
        await connection.close()
