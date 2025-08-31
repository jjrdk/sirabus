# SiraBus

[![Downloads](https://static.pepy.tech/badge/sirabus)](https://pepy.tech/project/sirabus)

SiraBus is a simple opinionated library for publishing and subscribing to events in an asynchronous and type-safe
manner (to the extent that type safety can be achieved in Python).

Users publish events to an `IPublishEvents` interface, and users can subscribe to events by passing instances of an
`IHandleEvents` interface to a `ServiceBus` implementation.

There's a `TopographyBuilder` that allows you to build a service bus topology, which is a declaration of queues and
exchanges (based on transport protocol) that the service bus will use. This is useful for setting up the service bus in
a production environment.

## Example Usage

```python
from sirabus import HierarchicalTopicMap, IHandleEvents
from sirabus.servicebus.amqp_servicebus import AmqpServiceBus, AmqpServiceBusConfiguration
import asyncio


class MyEventHandler(IHandleEvents):
    async def handle(self, event, headers):
        print(f"Handling event: {event} with headers: {headers}")


topic_map = HierarchicalTopicMap()
config = (AmqpServiceBusConfiguration.default()
    .with_amqp_url("amqp://guest:guest@localhost/")
    .with_topic_map(topic_map=topic_map)
    .with_handlers(MyEventHandler()))
# It can be further configured with SSL options, prefetch count, etc.
service_bus = AmqpServiceBus(
    configuration=config
)
asyncio.run(service_bus.run())
```

The `run` method starts the service bus and begins consuming messages from RabbitMQ.
The `stop` method should be called to gracefully shut down the service bus and close the connection to RabbitMQ.

The [message handling feature](https://github.com/jjrdk/sirabus/blob/master/tests/features/message_handling.feature) sets up a simple example of how to use the
library. It sets up a service bus, registers a handler for a specific event type, and then publishes an event.
The handler receives the event and processes it.

## Supported Message Transport Protocols

SiraBus supports the following message transport protocols:

| Protocol  | Description                        |
|-----------|------------------------------------|
| In-Memory | For local development and testing. |
| AMQP      | For production use with RabbitMQ.  |
| SQS       | For production use with AWS SQS.   |
| Redis     | For production use with Redis.     |

## Specific Topics

- [Service Bus](https://github.com/jjrdk/sirabus/blob/master/docs/service_bus.md): Overview of the service bus and its components.
- [Hierarchical Topics](https://github.com/jjrdk/sirabus/blob/master/docs/hierarchical_topics.md): Explanation of hierarchical topics and how to use them.
- [Event Handling](https://github.com/jjrdk/sirabus/blob/master/docs/event_handling.md): How to handle events using the service bus.
- [Command Handling](https://github.com/jjrdk/sirabus/blob/master/docs/command_handling.md): How to handle commands using the service bus.
