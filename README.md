# SiraBus

SiraBus is a simple opinionated library for publishing and subscribing to events in an asynchronous and type-safe 
manner (to the extent that type safety can be achieved in Python).

Users publish events to an `IPublishEvents` interface, and users can subscribe to events by passing instances of an 
`IHandleEvents` interface to a `ServiceBus` implementation.

## Example Usage

The [message handling feature](tests/features/message_handling.feature) sets up a simple example of how to use the 
library. It sets up a service bus, registers a handler for a specific event type, and then publishes an event. 
The handler receives the event and processes it.