from typing import Union

from loguru import logger

from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage


def check_resources(resource: Union[DynamoDBState, S3Storage]) -> bool:
    """
    Check if all required AWS resources exist.

    Args:
        state_manager: The DynamoDB state manager
        s3_storage: The S3 storage service

    Returns:
        True if all resources exist, False otherwise
    """
    if isinstance(resource, DynamoDBState):
        resource_exists = resource.check_resources_exist()
    elif isinstance(resource, S3Storage):
        resource_exists = resource.check_resources_exist()

    if not resource_exists:
        logger.error("DynamoDB resources do not exist or are not accessible")

    return resource_exists


def generate_guid_for_rss_entry(entry, feed_url, title=None):
    """
    Generate a guid (globally unique identifier) for an RSS entry.

    Args:
        entry: The feedparser entry object
        feed_url: The URL of the RSS feed
        title: Optional title to use if not available in the entry

    Returns:
        A string containing a unique identifier for the RSS entry
    """
    # Use entry's id if available
    guid = entry.get("id")

    # Fallback to link if id is not available
    if not guid:
        guid = entry.get("link")

    # Last resort - use feed URL and title
    if not guid:
        title = title or entry.get("title", "No Title Provided")
        guid = f"{feed_url}::{title}"

    return guid
