from datetime import datetime
from typing import List, Optional

import feedparser

from src.content_curator.config import config
from src.content_curator.fetchers.fetcher_base import Fetcher
from src.content_curator.models import ContentItem
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.utils import generate_guid_for_rss_entry


class RSSFetcher(Fetcher):
    """Fetcher implementation for RSS and Atom feeds."""

    def __init__(
        self,
        url_file_path: Optional[str] = None,
        max_items: Optional[int] = None,
        specific_url: Optional[str] = None,
        s3_storage: Optional[S3Storage] = None,
        state_manager: Optional[DynamoDBState] = None,
    ):
        """
        Initializes the RSS Fetcher.

        Args:
            url_file_path: Path to the text file containing RSS feed URLs (one per line).
            max_items: Maximum number of most recent items to fetch per feed. If None, fetch all items.
            specific_url: Optional specific URL to fetch, overrides url_file_path if provided.
            s3_storage: Optional S3Storage instance for storing HTML content.
            state_manager: Optional DynamoDBState instance for managing state.
        """
        source_identifier = (
            specific_url if specific_url else url_file_path or "direct_url"
        )
        super().__init__(source_identifier=source_identifier)
        self.url_file_path = url_file_path
        self.specific_url = specific_url
        self.max_items = max_items or config.rss_default_max_items
        self.s3_storage = s3_storage
        self.state_manager = state_manager

    def _read_urls_from_file(self) -> List[str]:
        """Gets URLs either from file or from specific_url parameter."""
        if self.specific_url:
            return [self.specific_url]

        elif self.url_file_path:
            with open(self.url_file_path, "r", encoding="utf-8") as f:
                # Process URLs one by one for better cleaning
                urls = []
                for line in f:
                    # Skip empty lines and comments
                    clean_line = line.strip()
                    if clean_line and not clean_line.startswith("#"):
                        # Ensure URL has no invisible characters or extra whitespace
                        clean_url = "".join(c for c in clean_line if c.isprintable())
                        urls.append(clean_url)

                self.logger.debug(f"Loaded {len(urls)} URLs from {self.url_file_path}")
                return urls

        else:
            raise ValueError("Either specific_url or url_file_path must be provided")

    def _extract_html_content(self, entry: feedparser.FeedParserDict) -> Optional[str]:
        """
        Attempts to extract the best HTML content from a feed entry.
        Checks common fields like 'content', 'summary_detail', 'description'.
        """
        if hasattr(entry, "content") and entry.content:
            # feedparser packs content into a list, get the primary value
            return entry.content[0].value
        elif (
            hasattr(entry, "summary_detail")
            and entry.summary_detail.type == "text/html"
        ):
            # Ensure it's actually HTML before using summary
            return entry.summary_detail.value
        elif hasattr(entry, "description"):  # Often contains the full HTML
            return entry.description
        elif hasattr(entry, "summary"):  # Less ideal, often truncated, but a fallback
            # Sometimes summary might be HTML, sometimes plain text. Markdownify handles plain text okay.
            return entry.summary

        # If no suitable content found
        title = entry.get("title", "N/A")
        link = entry.get("link", "N/A")
        self.logger.warning(
            f"Could not find suitable HTML content field for entry: '{title}' ({link})"
        )
        return None

    def fetch_items(self, specific_id: Optional[str] = None) -> List[ContentItem]:
        """
        Fetches items from all RSS feeds listed in the file or from specific_url.
        If s3_storage is provided, stores the HTML content in S3.

        Args:
            specific_id: Optional ID of a specific item to fetch

        Returns:
            List of ContentItem objects representing fetched content
        """
        feed_urls = self._read_urls_from_file()
        all_items: List[ContentItem] = []

        if not feed_urls:
            self.logger.warning("No feed URLs loaded, fetch aborted.")
            return all_items

        for url in feed_urls:
            self.logger.info(f"Processing feed: {url}")
            try:
                # Parse the feed URL
                feed_data = feedparser.parse(url)

                # Check if feedparser encountered issues (bozo means potential problem)
                if feed_data.bozo:
                    self.logger.warning(
                        f"Feed may be malformed: {url}. Reason: {feed_data.bozo_exception}"
                    )

                # Get entries and sort by published date if available
                entries = feed_data.entries
                if self.max_items is not None:
                    # Sort entries by published date if available, otherwise use updated date
                    entries.sort(
                        key=lambda x: x.get(
                            "published_parsed", x.get("updated_parsed", datetime.min)
                        ),
                        reverse=True,  # Most recent first
                    )
                    entries = entries[: self.max_items]
                    self.logger.info(
                        f"Limited to {self.max_items} most recent items for feed: {url}"
                    )

                self.logger.debug(f"Processing {len(entries)} entries in feed: {url}")

                for entry in entries:
                    title = entry.get("title", "No Title Provided")
                    link = entry.get(
                        "link", None
                    )  # Essential for fetching full page if needed later
                    published_parsed = entry.get(
                        "published_parsed", entry.get("updated_parsed", None)
                    )
                    published_date = None
                    if published_parsed:
                        # TODO: parse the published_parsed into a datetime object
                        # Format to ISO 8601 string or keep as struct_time
                        # For simplicity, let's use the raw string from feedparser if available
                        published_date = entry.get("published", entry.get("updated"))

                    # Generate guid using the utility function
                    guid = generate_guid_for_rss_entry(entry, url, title)

                    # Extract HTML content
                    html_content = self._extract_html_content(entry)

                    # Metadata for the item
                    fetch_date = datetime.now().isoformat()

                    # Define HTML path
                    html_path = f"html/{guid}.html"

                    # Store HTML content in S3 if storage is available
                    if html_content and self.s3_storage:
                        self.s3_storage.store_content(
                            html_path, html_content, content_type="text/html"
                        )
                        self.logger.info(f"Stored HTML content at: {html_path}")

                    # Create a ContentItem
                    item = ContentItem(
                        guid=guid,
                        link=link or "",  # Ensure link is never None
                        title=title,
                        published_date=published_date,
                        fetch_date=fetch_date,
                        source_url=url,
                        html_content=html_content,
                        html_path=html_path,
                    )

                    all_items.append(item)

                    self.logger.info(
                        f"Created new content item: '{item.title}' ({item.guid})"
                    )

            except Exception as e:
                self.logger.error(
                    f"Failed to fetch or process feed {url}: {e}", exc_info=True
                )
                # Continue to the next feed URL even if one fails

        self.logger.info(
            f"Finished processing feeds. Total items fetched: {len(all_items)}"
        )
        return all_items

    def fetch_and_update_state(
        self, specific_id: Optional[str] = None, overwrite_flag: bool = False
    ) -> List[ContentItem]:
        """
        Fetch content from RSS feeds and update the state in DynamoDB and S3.
        This method encapsulates the entire fetch stage logic.

        Args:
            specific_id: Optional ID of a specific item to fetch
            overwrite_flag: Whether to overwrite existing content

        Returns:
            List of fetched ContentItem objects
        """
        if not self.s3_storage or not self.state_manager:
            self.logger.error(
                "S3Storage and DynamoDBState are required for fetch_and_update_state"
            )
            return []

        fetched_items = []
        skipped_items = 0
        updated_items = 0
        new_items = 0

        # Fetch items from RSS feeds
        items = self.fetch_items(specific_id)
        if not items:
            self.logger.warning("No items fetched from RSS feeds")
            return []

        self.logger.info(f"--- Fetched {len(items)} items ---")

        for item in items:
            # Check if item already exists
            if self.state_manager.item_exists(item.guid):
                # Update existing item
                existing_item = self.state_manager.get_item(item.guid)
                if existing_item:
                    # Check if HTML content already exists
                    if existing_item.html_path and not overwrite_flag:
                        self.logger.debug(
                            f"Item {item.guid} already has HTML content at {existing_item.html_path}, will be preserved"
                        )
                        skipped_items += 1

                    # Store the current processing state via paths
                    md_path = existing_item.md_path
                    summary_path = existing_item.summary_path
                    short_summary_path = existing_item.short_summary_path
                    is_paywall = existing_item.is_paywall
                    to_be_summarized = existing_item.to_be_summarized
                    newsletters = existing_item.newsletters

                    # Update only fetch-related fields
                    existing_item.title = item.title
                    existing_item.link = item.link
                    existing_item.published_date = item.published_date
                    existing_item.fetch_date = item.fetch_date
                    existing_item.source_url = item.source_url
                    existing_item.html_path = item.html_path
                    existing_item.last_updated = datetime.now().isoformat()

                    # Only preserve processing state if not overwriting
                    if not overwrite_flag:
                        existing_item.md_path = md_path
                        existing_item.summary_path = summary_path
                        existing_item.short_summary_path = short_summary_path
                        existing_item.is_paywall = is_paywall
                        existing_item.to_be_summarized = to_be_summarized
                        existing_item.newsletters = newsletters

                    # Update the item in DynamoDB
                    self.state_manager.update_item(existing_item, overwrite_flag)
                    updated_items += 1
                    self.logger.debug(
                        f"Updated fetch metadata for item: {item.guid} (preserved processing paths)"
                    )
                    # Add the updated item to fetched_items
                    fetched_items.append(existing_item)
            else:
                # Store new item
                if self.state_manager.store_item(item):
                    new_items += 1
                    self.logger.debug(f"Stored new item: {item.guid}")
                    # Add the new item to fetched_items
                    fetched_items.append(item)

        # Log summary stats
        if skipped_items > 0:
            self.logger.warning(
                f"Skipped fetching HTML for {skipped_items} items that already had HTML content"
            )
        self.logger.info(
            f"Fetch summary: {new_items} new items, {updated_items} updated items, {skipped_items} skipped items"
        )

        return fetched_items


if __name__ == "__main__":
    # Test the RSS Fetcher
    # uv run python -m src.content_curator.fetchers.rss_fetcher
    # fetcher = RSSFetcher(url_file_path="data/rss_urls.txt", max_items=5)
    # items = fetcher.fetch_items()

    fetcher = RSSFetcher(specific_url="https://www.lesswrong.com/feed.xml")
    items = fetcher.fetch_items()

    # Print example of content item
    if items:
        print(f"Example item: {items[0].title}")
        print(f"Has HTML content: {'Yes' if items[0].html_content else 'No'}")
