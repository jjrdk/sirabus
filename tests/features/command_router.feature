Feature: Send commands and receive responses

  Scenario Template: Send an command and receive a response
    Given a running <broker_type> message broker
    And commands have been registered in the hierarchical topic map
    And a <serializer> <broker_type> service bus is configured with the hierarchical topic map
    And a <serializer> <broker_type> router is created with the hierarchical topic map
    When I send the command <command_type>
    Then I receive the <response_type> "<response_msg>"

    Examples:
      | broker_type | serializer | command_type    | response_type | response_msg    |
      | amqp        | cloudevent | get_status      | reply         | status: ok      |
      | amqp        | pydantic   | get_status      | reply         | status: ok      |
      | SQS         | cloudevent | get_status      | reply         | status: ok      |
      | SQS         | pydantic   | get_status      | reply         | status: ok      |
      | in-memory   | cloudevent | get_status      | reply         | status: ok      |
      | in-memory   | pydantic   | get_status      | reply         | status: ok      |
      | amqp        | cloudevent | invalid_command | error         | unknown command |
      | amqp        | pydantic   | invalid_command | error         | unknown command |
      | in-memory   | cloudevent | invalid_command | error         | unknown command |
      | in-memory   | pydantic   | invalid_command | error         | unknown command |
      | SQS         | cloudevent | invalid_command | error         | unknown command |
      | SQS         | pydantic   | invalid_command | error         | unknown command |

  Scenario Template: Send multiple commands and receive their responses
    Given a running <broker_type> message broker
    And commands have been registered in the hierarchical topic map
    And a <serializer> <broker_type> service bus is configured with the hierarchical topic map
    And a <serializer> <broker_type> router is created with the hierarchical topic map
    When I send the commands "get_status", "get_info"
    Then I receive the replies "status: ok", "info: system running"

    Examples:
      | broker_type | serializer |
      | amqp        | cloudevent |
      | amqp        | pydantic   |
      | SQS        | cloudevent |
      | SQS        | pydantic   |
      | in-memory   | cloudevent |
      | in-memory   | pydantic   |
