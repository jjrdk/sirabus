from aett.eventstore import Topic
from pydantic import BaseModel, Field


@Topic("command_response")
class CommandResponse(BaseModel):
    """
    Represents a response to a command.
    This class can be extended to provide specific response types.
    """

    success: bool = Field(
        default=True, description="Indicates if the command was successful"
    )
    message: str = Field(
        default="",
        description="A message providing additional information about the command response",
    )

    def __repr__(self) -> str:
        return f"CommandResponse(success={self.success}, message='{self.message}')"
