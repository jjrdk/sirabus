import logging

from behave.api.async_step import async_run_until_complete


def before_scenario(context, _):
    context.containers = []


@async_run_until_complete
async def after_scenario(context, _):
    for container in context.containers:
        try:
            container.stop()
        except Exception as e:
            logging.debug(f"Error stopping container {container}: {e}")
