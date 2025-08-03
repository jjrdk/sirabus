from steps.test_types import StatusCommand, InfoCommand
from sirabus import IHandleCommands, CommandResponse


class StatusCommandHandler(IHandleCommands[StatusCommand]):
    async def handle(self, command: StatusCommand, headers: dict) -> CommandResponse:
        return CommandResponse(success=True, message="status: ok")


class InfoCommandHandler(IHandleCommands[InfoCommand]):
    async def handle(self, command: InfoCommand, headers: dict) -> CommandResponse:
        return CommandResponse(success=True, message="info: system running")
