"""Utilities for content fetchers."""

import os
from typing import List, Optional

from loguru import logger


def read_urls_from_file(file_path: str) -> List[str]:
    """
    Reads URLs from a file, one URL per line.

    Args:
        file_path: Path to the text file containing URLs (one per line)

    Returns:
        List of URLs (with empty lines and comments removed)
    """
    urls = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                # Skip empty lines and lines starting with # (comments)
                if url and not url.startswith("#"):
                    urls.append(url)
        logger.info(f"Read {len(urls)} URLs from {file_path}")
        return urls
    except FileNotFoundError:
        logger.error(f"URL file not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading URL file {file_path}: {e}", exc_info=True)
        return []


def get_urls_for_fetch(
    url_file_path: Optional[str] = None, specific_url: Optional[str] = None
) -> List[str]:
    """
    Get URLs to fetch content from, either from a file or a single URL.

    Args:
        url_file_path: Path to file containing URLs
        specific_url: A single URL to process instead of using a file

    Returns:
        List of URLs to fetch
    """
    if specific_url:
        # Single URL provided
        logger.info(f"Using single URL: {specific_url}")
        return [specific_url]

    elif url_file_path and os.path.exists(url_file_path):
        # Read from file
        return read_urls_from_file(url_file_path)

    # No valid sources
    logger.warning("No valid URL source provided (neither file nor specific URL)")
    return []
