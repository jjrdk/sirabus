import logging

from behave.api.async_step import async_run_until_complete

from tests.features.steps.async_test_runner import AsyncTestRunner


def before_scenario(context, _):
    context.containers = []
    context.async_runner = AsyncTestRunner()


@async_run_until_complete
async def after_scenario(context, _):
    for container in context.containers:
        try:
            container.stop()
        except Exception as e:
            logging.debug(f"Error stopping container {container}: {e}")
