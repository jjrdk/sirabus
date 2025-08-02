Feature: Send commands and receive responses

  Scenario: Send a command and receive a response
    Given a running amqp message broker
    And commands have been registered in the hierarchical topic map
    And amqp router is configured with the hierarchical topic map
    When I send the command get_status
    Then I should receive the response status: ok

  Scenario: Send an invalid command and receive an error
    Given a running amqp message broker
    And commands have been registered in the hierarchical topic map
    And amqp router is configured with the hierarchical topic map
    When I send the command "invalid_command"
    Then I should receive an error "Unknown command"

  Scenario: Send multiple commands and receive their responses
    Given a running amqp message broker
    And commands have been registered in the hierarchical topic map
    And amqp router is configured with the hierarchical topic map
    When I send the commands "get_status", "get_info"
    Then I should receive the responses "status: ok", "info: system running"