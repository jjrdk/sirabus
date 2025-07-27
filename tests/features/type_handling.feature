Feature: Can resolve types correctly

  Scenario: Can resolve the event type for an event handler implementation
    Given a class with an event handler implementation
    When I resolve the event type for the handler
    Then the event type is resolved correctly