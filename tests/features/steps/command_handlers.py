from features.steps.test_types import StatusCommand
from sirabus import IHandleCommands, CommandResponse


class StatusCommandHandler(IHandleCommands[StatusCommand]):
    async def handle(self, command: StatusCommand, headers: dict) -> CommandResponse:
        return CommandResponse(success=True, message="status: ok")
