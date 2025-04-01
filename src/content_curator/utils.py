import base64
import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Union

from dateutil import parser as date_parser
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


def parse_date(date_string: str, verbose: bool = False) -> Optional[datetime]:
    """
    Parse a date string into a datetime object, handling multiple formats.

    This function tries multiple parsing methods to handle various date formats:
    1. ISO 8601 (e.g., "2023-06-22T13:44:50Z")
    2. RFC 2822 (e.g., "Wed, 22 Jun 2023 13:44:50 GMT") - common in RSS feeds
    3. Common date formats using dateutil parser as a fallback

    All returned datetime objects are timezone-aware with UTC timezone
    to ensure consistent comparisons.

    Args:
        date_string: The date string to parse
        verbose: If True, logs detailed debug information during parsing

    Returns:
        A timezone-aware datetime object if parsing succeeds, None otherwise
    """
    if not date_string:
        return None

    if verbose:
        logger.debug(f"Attempting to parse date: '{date_string}'")

    # Keep track of errors for debugging
    errors = []

    # Try ISO format first (fastest and most precise)
    if "T" in date_string:
        try:
            if verbose:
                logger.debug(f"Trying ISO format parser for: '{date_string}'")
            # Handle trailing Z (UTC timezone) by replacing with +00:00
            iso_date = date_string.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_date)
            # Ensure timezone is set
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if verbose:
                logger.debug(f"ISO format parser succeeded: {dt}")
            return dt
        except ValueError as e:
            error_msg = f"ISO format parse failed: {str(e)}"
            if verbose:
                logger.debug(error_msg)
            errors.append(error_msg)

    # Try RFC 2822 format (common in RSS feeds)
    if any(
        day in date_string for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    ):
        try:
            if verbose:
                logger.debug(f"Trying RFC 2822 parser for: '{date_string}'")
            dt = parsedate_to_datetime(date_string)
            # RFC 2822 dates from parsedate_to_datetime are already timezone-aware
            if verbose:
                logger.debug(f"RFC 2822 parser succeeded: {dt}")
            return dt
        except Exception as e:
            error_msg = f"RFC 2822 parse failed: {str(e)}"
            if verbose:
                logger.debug(error_msg)
            errors.append(error_msg)

    # Try common datetime formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ]

    for format_str in formats:
        try:
            if verbose:
                logger.debug(f"Trying format '{format_str}' for: '{date_string}'")
            dt = datetime.strptime(date_string, format_str)
            # These formats don't include timezone, so add UTC
            dt = dt.replace(tzinfo=timezone.utc)
            if verbose:
                logger.debug(f"Format '{format_str}' succeeded: {dt}")
            return dt
        except ValueError:
            pass  # Try the next format without logging each failure

    # Last resort - use dateutil parser which can handle many formats
    try:
        if verbose:
            logger.debug(f"Trying dateutil parser for: '{date_string}'")

        dt = date_parser.parse(date_string)
        # Add timezone if not present
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if verbose:
            logger.debug(f"dateutil parser succeeded: {dt}")
        return dt
    except Exception as e:
        error_msg = f"dateutil parse failed: {str(e)}"
        if verbose:
            logger.debug(error_msg)
        errors.append(error_msg)

    # If all parsing methods fail, log the error and return None
    logger.warning(
        f"Failed to parse date string: '{date_string}'. Errors: {', '.join(errors)}"
    )
    return None


def format_date_iso(dt: Optional[datetime] = None) -> str:
    """
    Format a datetime object as an ISO 8601 string.

    Args:
        dt: The datetime object to format (defaults to current time if None)

    Returns:
        An ISO 8601 formatted date string
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        # Make timezone-aware if it isn't already
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
