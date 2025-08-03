import threading

from behave import given, then, when, use_step_matcher

from test_types import TestEventHandler, TestEvent

use_step_matcher("re")


@given("a class with an event handler implementation")
def step_impl1(context):
    context.instance = TestEventHandler(threading.Event())


@when("I resolve the event type for the handler")
def step_impl2(context):
    context.event_type = type(context.instance).message_type


@then("the event type is resolved correctly")
def step_impl3(context):
    assert context.event_type == TestEvent, (
        f"Expected {TestEvent}, got {context.event_type}"
    )
