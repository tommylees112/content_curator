import base64
import hashlib
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional, Tuple, Union

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


def _check_paywall_patterns(
    sample_text: str, paywall_patterns: List[str] = None
) -> Tuple[bool, str]:
    """
    Check for paywall patterns in the sample text.

    Args:
        sample_text: The text to check for paywall patterns
        paywall_patterns: List of patterns to check. If None, uses default patterns

    Returns:
        Tuple of (found_pattern: bool, matched_pattern: str)
    """
    # Use default patterns if none provided
    if paywall_patterns is None:
        paywall_patterns = [
            r"subscribe now",
            r"subscribe to continue",
            r"subscribe for full access",
            r"read more",
            r"to continue reading",
            r"sign up",
            r"login to continue",
            r"premium content",
            r"become a member",
            r"for subscribers only",
            r"this content is available to subscribers",
        ]

    # Check for paywall patterns
    for pattern in paywall_patterns:
        if re.search(pattern, sample_text, re.IGNORECASE):
            return True, pattern
    return False, ""


def is_paywall_or_teaser(
    markdown_content: str,
    min_content_length: int = 100,
    paywall_patterns: List[str] = None,
    max_link_ratio: float = 0.3,
    min_failures_to_reject: int = 2,
) -> bool:
    """
    Detect if content appears to be behind a paywall or is just a teaser.

    The function performs three quality checks:
    1. Content Length: Fails if content is shorter than min_content_length (default: 100 chars)
    2. Paywall Patterns: Fails if any paywall-related phrases are found in the first 500 chars
    3. Link Ratio: Fails if the ratio of markdown links to text length exceeds max_link_ratio (default: 0.3)

    Content is marked as paywall/teaser if at least min_failures_to_reject checks fail.

    Args:
        markdown_content: The markdown content to check
        min_content_length: Minimum text length (in chars) to not be considered too short
        paywall_patterns: List of regex patterns to detect paywall phrases. If None, uses default patterns
        max_link_ratio: Maximum allowed ratio of markdown links to text length
        min_failures_to_reject: Minimum number of quality checks that must fail to mark as paywall/teaser

    Returns:
        True if content appears to be a teaser or behind a paywall
    """
    # Remove header metadata lines if present
    content_lines = markdown_content.strip().split("\n")
    content_body = "\n".join(
        [
            line
            for line in content_lines
            if not line.startswith("Date ")
            and not line.startswith("Title:")
            and not line.startswith("URL Source:")
        ]
    )

    # Strip markdown and get pure text for length check
    text_only = re.sub(r"\[.*?\]\(.*?\)", "", content_body)  # Remove markdown links
    text_only = re.sub(r"[#*_`]", "", text_only)  # Remove markdown formatting
    clean_text = text_only.strip()

    # Track failed checks
    failed_checks = 0

    # Check for very short content
    if len(clean_text) < min_content_length:
        logger.warning(
            f"Content detected as too short: {len(clean_text)} chars (minimum: {min_content_length})"
        )
        failed_checks += 1

    # Get text to check for paywall patterns - just the first few paragraphs
    sample_text = clean_text[:500].lower()

    # Check for paywall patterns
    found_pattern, matched_pattern = _check_paywall_patterns(
        sample_text, paywall_patterns
    )
    if found_pattern:
        logger.info(f"Found paywall pattern: '{matched_pattern}'")
        failed_checks += 1

    # Check link ratio
    link_ratio = len(re.findall(r"\[.*?\]\(.*?\)", content_body)) / max(
        1, len(clean_text) / 100
    )
    if link_ratio > max_link_ratio:
        logger.info(
            f"Content has high link ratio: {link_ratio:.2f} (maximum: {max_link_ratio})"
        )
        failed_checks += 1

    # Only mark as paywall if enough checks failed
    if failed_checks >= min_failures_to_reject:
        logger.info(
            f"Content marked as paywall/teaser: {failed_checks} checks failed (minimum {min_failures_to_reject} required)"
        )
        return True

    return False


def is_worth_summarizing(
    markdown_content: str,
    min_content_length: int = 500,
    max_punctuation_ratio: float = 0.05,  # 1 ! or ? per 20 chars
    min_sentences: int = 5,
    min_paragraphs: int = 3,
    min_failures_to_reject: int = 3,  # Number of checks that must fail to reject content
) -> bool:
    """
    Determine if content is worth summarizing based on length and quality heuristics.

    Args:
        markdown_content: The markdown content to check
        min_content_length: Minimum text length to consider summarizing
        max_punctuation_ratio: Maximum allowed ratio of ! or ? to total characters
        min_sentences: Minimum number of sentences required
        min_paragraphs: Minimum number of paragraphs required
        min_failures_to_reject: Minimum number of quality checks that must fail to reject content

    Returns:
        True if content is worth summarizing
    """
    # Skip paywall/teaser content - this is an automatic rejection
    if is_paywall_or_teaser(markdown_content):
        logger.info("Content is behind paywall or just a teaser, skipping")
        return False

    # Remove header metadata lines if present
    content_lines = markdown_content.strip().split("\n")
    content_body = "\n".join(
        [
            line
            for line in content_lines
            if not line.startswith("Date ")
            and not line.startswith("Title:")
            and not line.startswith("URL Source:")
        ]
    )

    # Strip markdown and get pure text for length check
    text_only = re.sub(r"\[.*?\]\(.*?\)", "", content_body)  # Remove markdown links
    text_only = re.sub(r"[#*_`]", "", text_only)  # Remove markdown formatting
    clean_text = text_only.strip()

    # Count failed checks
    failed_checks = 0

    # Check for minimum content length
    if len(clean_text) < min_content_length:
        logger.info(f"Content too short for summarization: {len(clean_text)} chars")
        failed_checks += 1

    # Check for excessive punctuation or unusual patterns
    punct_count = len(re.findall(r"[!?]", clean_text))
    punct_ratio = punct_count / max(1, len(clean_text))
    if punct_ratio > max_punctuation_ratio:
        logger.info(
            f"Content has excessive punctuation: {punct_count} out of {len(clean_text)} chars ({punct_ratio:.4f})"
        )
        failed_checks += 1

    # Count sentences as a rough proxy for article development
    sentences = re.split(r"[.!?]+", clean_text)
    if len(sentences) < min_sentences:
        logger.info(
            f"Content has too few sentences: {len(sentences)} < {min_sentences}"
        )
        failed_checks += 1

    # Count paragraphs
    paragraphs = [p for p in content_body.split("\n\n") if p.strip()]
    if len(paragraphs) < min_paragraphs:
        logger.info(
            f"Content has too few paragraphs: {len(paragraphs)} < {min_paragraphs}"
        )
        failed_checks += 1

    # Check if enough checks failed to reject the content
    if failed_checks >= min_failures_to_reject:
        logger.info(
            f"Content failed {failed_checks} quality checks, minimum {min_failures_to_reject} required to reject"
        )
        return False

    return True
