Feature: Event Handling

  Scenario Template: Message publishing and subscription
    Given a running <broker_type> message broker
    And events have been registered in the hierarchical topic map
    And a <serializer> <broker_type> service bus is configured with the hierarchical topic map
    When I send a <serializer> <event_type> message to the <broker_type> service bus
    Then the message is received by the subscriber
    And the other event handlers are not invoked

    Examples:
      | broker_type | serializer | event_type |
      | amqp        | cloudevent | nested     |
      | amqp        | cloudevent | test       |
      | amqp        | pydantic   | nested     |
      | amqp        | pydantic   | test       |
      | in-memory   | cloudevent | nested     |
      | in-memory   | cloudevent | test       |
      | in-memory   | pydantic   | nested     |
      | in-memory   | pydantic   | test       |
      | SQS         | cloudevent | nested     |
      | SQS         | cloudevent | test       |
      | SQS         | pydantic   | nested     |
      | SQS         | pydantic   | test       |

  Scenario Template: Multiple event handling segregation
    Given a running <broker_type> message broker
    And events have been registered in the hierarchical topic map
    And a <serializer> <broker_type> service bus is configured with the hierarchical topic map
    When I send a <serializer> <event_type> message to the <broker_type> service bus
    When I send a <serializer> other message to the <broker_type> service bus
    Then the messages are received by the subscriber

    Examples:
      | broker_type | serializer | event_type |
      | amqp        | cloudevent | nested     |
      | amqp        | cloudevent | test       |
      | amqp        | pydantic   | nested     |
      | amqp        | pydantic   | test       |
      | in-memory   | cloudevent | nested     |
      | in-memory   | cloudevent | test       |
      | in-memory   | pydantic   | nested     |
      | in-memory   | pydantic   | test       |
      | SQS         | cloudevent | nested     |
      | SQS         | cloudevent | test       |
      | SQS         | pydantic   | nested     |
      | SQS         | pydantic   | test       |
