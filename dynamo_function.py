"""
AWS Lambda function that processes DynamoDB stream events.
"""

import json
from logger_setup import get_logger
from typing import Dict, Any

logger = get_logger("dynamo")

def dynamo_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function for processing DynamoDB stream events.

    Args:
        event: The event dict that contains the DynamoDB stream records
        context: The context object that contains information about the runtime

    Returns:
        Dict containing the processing results
    """
    try:
        records = event.get("Records", [])
        logger.info("Processing DynamoDB stream records", records=records)
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "DynamoDB stream records processed"}),
        }
    except Exception as e:
        logger.error("Error in dynamo handler", error=e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "message": str(e)}),
        }
