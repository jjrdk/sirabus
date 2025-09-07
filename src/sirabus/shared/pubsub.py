from typing import Optional

from google.pubsub_v1 import PubsubMessage


def create_pubsub_message(
    data: bytes,
    hierarchical_topic: str,
    correlation_id: Optional[str] = None,
    message_id: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> PubsubMessage:
    msg = PubsubMessage()
    msg.data = data
    msg.attributes = {"topic": hierarchical_topic}
    if correlation_id:
        msg.attributes["correlation_id"] = correlation_id
    if message_id:
        msg.attributes["message_id"] = message_id
    if reply_to:
        msg.attributes["reply_to"] = reply_to
    return msg
