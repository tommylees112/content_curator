# --- fetcher_base.py ---
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from loguru import logger


class Fetcher(ABC):
    """Abstract base class for all content fetchers."""

    def __init__(self, source_identifier: str) -> None:
        """
        Initializes the fetcher.

        Args:
            source_identifier: A unique name or identifier for the source being fetched
                               (e.g., file path, API endpoint root, channel name).
        """
        self.source_identifier: str = source_identifier
        self.logger = logger
        self.logger.info(f"Initializing fetcher for source: {source_identifier}")

    @abstractmethod
    def fetch_items(self) -> List[Dict[str, Any]]:
        """
        Fetches new items from the source and processes them.

        Returns:
            A list of dictionaries, where each dictionary represents an item
            and should contain at least 'guid' (a unique ID for the item)
            and relevant metadata (like 'title', 'link', 'published_date', 'source_url', 'html_content').
        """
        pass

    def run(self) -> List[Dict[str, Any]]:
        """
        Public method to execute the fetching process.

        Returns:
            A list of dictionaries containing the fetched items with their metadata
        """
        self.logger.info(f"Running fetch for source: {self.source_identifier}")
        try:
            items = self.fetch_items()
            self.logger.info(
                f"Successfully fetched {len(items)} items for source: {self.source_identifier}"
            )
            return items
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred during fetch for {self.source_identifier}: {e}",
                exc_info=True,
            )
            return []  # Return empty list on major failure
