# Hierarchical Topic Distribution

In the hierarchical topic distribution model, class inheritance is leveraged to construct a topic topology that mirrors the inheritance tree of message types. This approach enables flexible and powerful message routing based on the relationships between classes.

## Class Inheritance as Topic Topology

Each message type (class) defines a topic. When classes are organized in an inheritance hierarchy, this structure is reflected in the topic topology:

- **Parent classes** represent broader topics.
- **Child classes** represent more specific topics that inherit from their parents.

This means the topic tree follows the class inheritance tree. For example:

```
BaseEvent
│
├── UserEvent
│   ├── UserCreatedEvent
│   └── UserDeletedEvent
└── SystemEvent
    └── SystemErrorEvent
```

## Subscription Propagation

When a subscriber subscribes to a parent class (topic), it automatically receives messages of all child classes (subtopics). This is because a message of a child class is also an instance of its parent class, according to the rules of inheritance.

- **Subscribing to `BaseEvent`**: Receives all messages of type `BaseEvent`, `UserEvent`, `UserCreatedEvent`, `UserDeletedEvent`, `SystemEvent`, and `SystemErrorEvent`.
- **Subscribing to `UserEvent`**: Receives messages of type `UserEvent`, `UserCreatedEvent`, and `UserDeletedEvent`.
- **Subscribing to `UserCreatedEvent`**: Receives only messages of that specific type.

## Example

Suppose you have the following class definitions:

```python
class BaseEvent: pass
class UserEvent(BaseEvent): pass
class UserCreatedEvent(UserEvent): pass
class UserDeletedEvent(UserEvent): pass
```

If a handler subscribes to `UserEvent`, it will receive messages of type `UserEvent`, `UserCreatedEvent`, and `UserDeletedEvent`. This allows for both broad and fine-grained message handling, depending on the level of the hierarchy you subscribe to.

This hierarchical approach enables scalable and maintainable message routing, as new message types can be added to the hierarchy without disrupting existing subscriptions.
