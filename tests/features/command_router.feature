Feature: Send commands and receive responses

  Scenario: Send an AMQP command and receive a response
    Given a running amqp message broker
    And commands have been registered in the AMQP hierarchical topic map
    And amqp router is configured with the hierarchical topic map
    When I send the command get_status
    Then I should receive the reply "status: ok"

  Scenario: Send an in-memory command and receive a response
    Given a running in-memory message broker
    And in-memory broker is configured with the hierarchical topic map
    And commands have been registered in the in-memory hierarchical topic map
    When I send the command get_status
    Then I should receive the reply "status: ok"

  Scenario: Send an invalid command and receive an error
    Given a running amqp message broker
    And commands have been registered in the AMQP hierarchical topic map
    And amqp router is configured with the hierarchical topic map
    When I send the command invalid_command
    Then I should receive the error "unknown command"

  Scenario: Send multiple commands and receive their responses
    Given a running amqp message broker
    And commands have been registered in the AMQP hierarchical topic map
    And amqp router is configured with the hierarchical topic map
    When I send the commands "get_status", "get_info"
    Then I should receive the replies "status: ok", "info: system running"