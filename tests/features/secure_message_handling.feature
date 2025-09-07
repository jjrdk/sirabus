Feature: Secure Event Handling

  Scenario Template: Secure message publishing and subscription
    Given a running <broker_type> message broker with TLS enabled
    And events have been registered in the hierarchical topic map
    And a <serializer> <broker_type> service bus with TLS enabled is configured with the hierarchical topic map
    When I send a <serializer> <event_type> message to the <broker_type> service bus with TLS enabled
    Then the message is received by the subscriber
    And the other event handlers are not invoked

    Examples:
      | broker_type | serializer | event_type           |
      | amqp        | cloudevent | test.test_sub.nested |
      | amqp        | cloudevent | test                 |
      | amqp        | pydantic   | test.test_sub.nested |
      | amqp        | pydantic   | test                 |
      | redis       | cloudevent | test.test_sub.nested |
      | redis       | cloudevent | test                 |
      | redis       | pydantic   | test.test_sub.nested |
      | redis       | pydantic   | test                 |
      | SQS         | cloudevent | test.test_sub.nested |
      | SQS         | cloudevent | test                 |
      | SQS         | pydantic   | test.test_sub.nested |
      | SQS         | pydantic   | test                 |
#      | pubsub      | Cannot create separate TLS instances for publisher and subscriber in tests | N/A             |

  Scenario Template: Multiple event handling segregation over TLS
    Given a running <broker_type> message broker with TLS enabled
    And events have been registered in the hierarchical topic map
    And a <serializer> <broker_type> service bus with TLS enabled is configured with the hierarchical topic map
    When I send a <serializer> <event_type> message to the <broker_type> service bus with TLS enabled
    When I send a <serializer> other message to the <broker_type> service bus with TLS enabled
    Then the messages are received by the subscriber

    Examples:
      | broker_type | serializer | event_type           |
      | amqp        | cloudevent | test.test_sub.nested |
      | amqp        | cloudevent | test                 |
      | amqp        | pydantic   | test.test_sub.nested |
      | amqp        | pydantic   | test                 |
      | redis       | cloudevent | test.test_sub.nested |
      | redis       | cloudevent | test                 |
      | redis       | pydantic   | test.test_sub.nested |
      | redis       | pydantic   | test                 |
      | SQS         | cloudevent | test.test_sub.nested |
      | SQS         | cloudevent | test                 |
      | SQS         | pydantic   | test.test_sub.nested |
      | SQS         | pydantic   | test                 |
