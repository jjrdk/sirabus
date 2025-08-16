# Service Bus

# Service Bus Overview

The Service Bus is a core component responsible for routing, delivering, and managing messages (events and commands) between different parts of a distributed system. It abstracts the underlying transport mechanism and provides a unified interface for message handling, subscription management, and response coordination.

## How the Service Bus Works

1. **Initialization**
   - The Service Bus is initialized with a hierarchical topic map, a message reader, a list of handlers (for events and commands), and a logger.
   - The topic map defines how message types are mapped to topics, supporting inheritance-based routing.

2. **Message Handling**
   - Incoming messages are processed by the `handle_message` method, which uses the message reader to deserialize the message and determine its type (event or command).
   - If the message is an event, it is dispatched to all registered event handlers that can process that event type or its parent types (using the topic map and class inheritance).
   - If the message is a command, it is routed to the appropriate command handler. Commands require a response, which is sent back to the requester via the `send_command_response` method.

3. **Event Handling**
   - Events are broadcast to all matching handlers which handle the event type. The Service Bus uses asynchronous execution to deliver events to multiple handlers in parallel.
   - Event handlers do not return a response to the sender; they simply process the event.

4. **Command Handling**
   - Commands are routed to a single handler. The handler processes the command and returns a response (success, failure, or result).
   - The Service Bus ensures that the response is sent back to the requester, handling correlation IDs and reply addresses.

5. **Subscription Management**
   - The Service Bus automatically manages subscriptions based on the registered handlers and the topic map. This ensures that handlers receive the correct messages without manual subscription logic.

6. **Acknowledgement and Error Handling**
   - The Service Bus manages message acknowledgement, ensuring that messages are only removed from the queue after successful processing.
   - If processing fails, the message can be re-queued or dead-lettered according to configuration.

## Extensibility

The Service Bus is designed to be transport-agnostic. Different implementations (e.g., AMQP, SQS, in-memory) can be plugged in by subclassing the abstract ServiceBus class and implementing the required methods (`run`, `stop`, `send_command_response`).

This architecture enables scalable, reliable, and maintainable message-driven systems, decoupling business logic from transport and infrastructure concerns.
