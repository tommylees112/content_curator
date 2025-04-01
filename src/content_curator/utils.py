import base64
import hashlib
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


def generate_url_hash(url: str) -> str:
    """
    Generate a consistent, short hash for a URL that is easy to copy and use as an ID.

    Args:
        url: The URL to generate a hash for

    Returns:
        A short, consistent hash string (approximately 11 characters, URL-safe)
    """
    # Remove any trailing slashes and normalize to lowercase
    normalized_url = url.rstrip("/").lower()

    # Create SHA-256 hash of the normalized URL
    hash_obj = hashlib.sha256(normalized_url.encode())

    # Get first 8 bytes of hash and encode in base64
    # This gives us a ~11 character string that is URL-safe
    short_hash = base64.urlsafe_b64encode(hash_obj.digest()[:8]).decode().rstrip("=")

    return short_hash


def generate_guid_for_rss_entry(entry, feed_url, title=None):
    """
    Generate a guid (globally unique identifier) for an RSS entry.

    Args:
        entry: The feedparser entry object
        feed_url: The URL of the RSS feed
        title: Optional title to use if not available in the entry

    Returns:
        A string containing a unique identifier for the RSS entry (URL-safe hash)
    """
    # Use entry's id if available
    url = entry.get("id")

    # Fallback to link if id is not available
    if not url:
        url = entry.get("link")

    # Last resort - use feed URL and title
    if not url:
        title = title or entry.get("title", "No Title Provided")
        url = f"{feed_url}::{title}"

    # Generate consistent hash for the URL
    return generate_url_hash(url)
