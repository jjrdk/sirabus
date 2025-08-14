Feature: Event Handling

  Scenario: AMQP message publishing and subscription with cloudevents
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And a cloudevent amqp broker is configured with the hierarchical topic map
    When I send a cloudevent nested message to the amqp service bus
    Then the message is received by the subscriber

  Scenario: SQS message publishing and subscription with cloudevents
    Given a running SQS message broker
    And events have been registered in the hierarchical topic map
    And a cloudevent SQS broker is configured with the hierarchical topic map
    When I send a cloudevent nested message to the SQS service bus
    Then the message is received by the subscriber

  Scenario: AMQP message publishing and subscription with pydantic
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And a pydantic amqp broker is configured with the hierarchical topic map
    When I send a pydantic test message to the amqp service bus
    Then the message is received by the subscriber

  Scenario: In memory message publishing and subscription with cloudevents
    Given a running in-memory message broker
    And events have been registered in the hierarchical topic map
    And a cloudevent in-memory broker is configured with the hierarchical topic map
    When I send a cloudevent test message to the in-memory service bus
    Then the message is received by the subscriber

  Scenario: In memory message publishing and subscription with pydantic
    Given a running in-memory message broker
    And events have been registered in the hierarchical topic map
    And a pydantic in-memory broker is configured with the hierarchical topic map
    When I send a pydantic test message to the in-memory service bus
    Then the message is received by the subscriber

  Scenario: Event handling segregation
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And a cloudevent amqp broker is configured with the hierarchical topic map
    When I send a cloudevent test message to the amqp service bus
    Then the message is received by the subscriber
    And the other event handlers are not invoked

  Scenario: Multiple event handling segregation
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And a cloudevent amqp broker is configured with the hierarchical topic map
    When I send a cloudevent test message to the amqp service bus
    When I send a cloudevent other message to the amqp service bus
    Then the messages are received by the subscriber
