# Handling Events

## Implementing the `IHandleEvents` Interface

To handle events in the system, you must implement the `IHandleEvents` interface. This interface defines the contract 
for event handlers, ensuring that your handler can process incoming events of specific types.

### Steps to Handle Events

1. **Implement the Interface**
   - Create a class that implements the `IHandleEvents` interface.
   - Define the event types your handler will process, typically by implementing a method such as `handle(event)` or 
     similar, depending on the interface definition.

2. **Register the Handler with the Service Bus**
   - Pass your handler instance to the service bus when initializing it.
   - The service bus inspects the handler to determine which event types it can process.

3. **Automatic Subscription Management**
   - The service bus creates the necessary subscriptions for the event types handled by your handler.
   - When an event of a matching type is received, the service bus delivers it to your handler's method.

4. **Event Acknowledgement and Re-queuing**
   - After your handler processes an event, the service bus manages acknowledgement (ACK) of the event.
   - If processing fails or an exception is raised, the service bus can return the event to the queue for retry or 
     dead-lettering, depending on configuration.

### Example

```python
from sirabus import IHandleEvents
from aett.eventstore import BaseEvent
from pydantic import Field

class MyEvent(BaseEvent):
    data: str = Field()
    
    
class MyEventHandler(IHandleEvents):
    def handle(self, event: MyEvent, headers: dict):
        # Process the event
        # Raise an exception if processing fails to trigger re-queuing if supported by the transport.
        ...

# Add the handler to the service bus configuration
```

In this example, `MyEventHandler` implements the `IHandleEvents` interface and defines how to process `MyEvent` 
instances. 
The handler is registered with the service bus, which takes care of subscriptions and event delivery.

This design decouples event processing logic from transport and subscription management, allowing you to focus on 
business logic while the service bus handles delivery guarantees and error handling.
