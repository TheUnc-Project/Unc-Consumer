import httpx
import asyncio
from logger_setup import get_logger

logger = get_logger("reply")


async def notify_reply_service(sender_id: str) -> None:
    url = "https://intelligence.theunproject.com/reply"
    payload = {"sender_id": sender_id}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload)

        if response.status_code == 200:
            logger.info("Successfully notified reply service", sender_id=sender_id)
        else:
            logger.warning(
                "Reply service returned non-200",
                sender_id=sender_id,
                status_code=response.status_code,
                response_body=response.text,
            )
    except Exception as e:
        logger.error(
            "Failed to notify reply service", sender_id=sender_id, error=str(e)
        )
