import asyncio
from typing import Dict, Any, Set
from boto3.dynamodb.types import TypeDeserializer

import config
from logger_setup import get_logger
from handlers.dynamo_event_handler import notify_reply_service

logger = get_logger("dynamo")
deserializer = TypeDeserializer()


def dynamo_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info("Handling DynamoDB event", request_id=context.aws_request_id)

        records = event.get("Records", [])
        if not records:
            logger.info("No records found in event")
            return {"statusCode": 200, "body": "No records to process"}

        sender_ids: Set[str] = set()

        for record in records:
            if record.get("eventName") == "INSERT":
                new_image = record.get("dynamodb", {}).get("NewImage", {})
                if new_image:
                    unmarshalled = {
                        k: deserializer.deserialize(v) for k, v in new_image.items()
                    }
                    sender_id = unmarshalled.get("sender_id")
                    if sender_id:
                        sender_ids.add(sender_id)
                    else:
                        logger.warning(
                            "sender_id not found in record", record=unmarshalled
                        )

        # Fire off all notify calls in parallel
        asyncio.run(asyncio.gather(*[notify_reply_service(sid) for sid in sender_ids]))

        return {
            "statusCode": 200,
            "body": "DynamoDB stream processed",
        }

    except Exception as e:
        logger.error("Unhandled error in handler", error=str(e))
        return {
            "statusCode": 500,
            "body": "Internal server error",
        }
