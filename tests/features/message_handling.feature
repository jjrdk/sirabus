Feature: Event Handling

  Scenario: AMQP message publishing and subscription
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And amqp broker is configured with the hierarchical topic map
    When I send a test message to the amqp service bus
    Then the message is received by the subscriber

  Scenario: In memory message publishing and subscription
    Given a running in-memory message broker
    And events have been registered in the hierarchical topic map
    And in-memory broker is configured with the hierarchical topic map
    When I send a test message to the in-memory service bus
    Then the message is received by the subscriber

  Scenario: Event handling segregation
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And amqp broker is configured with the hierarchical topic map
    When I send a test message to the amqp service bus
    Then the message is received by the subscriber
    And the other event handlers are not invoked

  Scenario: Event handling segregation
    Given a running amqp message broker
    And events have been registered in the hierarchical topic map
    And amqp broker is configured with the hierarchical topic map
    When I send a test message to the amqp service bus
    When I send an other message to the amqp service bus
    Then the messages are received by the subscriber
