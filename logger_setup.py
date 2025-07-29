"""
Logger setup module for AWS Lambda functions.
"""

import logging
import sys
from typing import Any


class LambdaLogFormatter(logging.Formatter):
    """Custom formatter that includes additional context."""

    def format(self, record: logging.LogRecord) -> str:
        # Get the original format
        record_dict = record.__dict__

        # Add extra fields if they exist
        extra = {
            key: value
            for key, value in record_dict.items()
            if key not in logging.LogRecord("").__dict__ and not key.startswith("_")
        }

        # Add the extra fields to the message
        if extra:
            record.msg = f"{record.msg} | Extra: {extra}"

        return super().format(record)


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Name for the logger

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)

    # Only add handler if logger doesn't have handlers
    if not logger.handlers:
        # Set log level to capture everything
        logger.setLevel(logging.DEBUG)

        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)

        # Create formatter
        formatter = LambdaLogFormatter(
            "[%(levelname)s] %(asctime)s | %(name)s | %(message)s"
        )

        # Add formatter to handler
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(handler)

        # Prevent propagation to root logger
        logger.propagate = False

    return logger
