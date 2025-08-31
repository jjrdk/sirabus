# Command Handling

## Commands Require a Response

In the command handling model, commands are messages that represent requests for an action to be performed. 
Unlike events, commands are directed to a single handler and always require a response. 
The response can indicate success, failure, or return a result from the command execution. 
This request-response pattern ensures that the sender knows the outcome of the command.

## Implementing the `IHandleCommands` Interface

To handle commands, you must implement the `IHandleCommands` interface. This interface defines how a handler processes 
a command and returns a response. Typically, the interface requires a method such as `handle(command)` that processes 
the command and returns a result or raises an exception on failure.

### Example Implementation

```python
from sirabus import IHandleCommands, CommandResponse
from aett.eventstore import BaseCommand
from pydantic import Field

class MyCommand(BaseCommand):
    data: str = Field(description="Some data")

class MyCommandHandler(IHandleCommands):
    def handle(self, command: MyCommand, headers: dict) -> CommandResponse:
        # Process the command
        return CommandResponse(success=True, message="Ok")
```

### Registering the Handler

Register your command handler with the service bus when initializing it. The service bus will inspect the handler to 
determine which command types it can process.

When a command is sent, the service bus routes it to the appropriate handler, waits for the response, and returns the 
result to the sender. If the handler raises an exception, the service bus can propagate the error back to the sender.

This pattern ensures reliable, asynchronous processing of commands with clear feedback to the sender.
