from typing import List, Optional

from langchain_community.document_transformers import MarkdownifyTransformer
from langchain_core.documents import Document
from loguru import logger

from src.content_curator.models import ContentItem
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.utils import is_paywall_or_teaser


class MarkdownProcessor:
    """Handles content processing tasks like HTML to Markdown conversion and summarization."""

    def __init__(
        self,
        min_content_length: int = 500,
        s3_storage: Optional[S3Storage] = None,
        state_manager: Optional[DynamoDBState] = None,
    ):
        """
        Initialize the content processor with necessary transformers.

        Args:
            min_content_length: Minimum text length (in characters) to be considered worth summarizing
            s3_storage: Optional S3Storage instance for retrieving and storing content
            state_manager: Optional DynamoDBState instance for updating item state
        """
        self.logger = logger
        self.min_content_length = min_content_length
        self.s3_storage = s3_storage
        self.state_manager = state_manager
        # Initialize MarkdownifyTransformer with proper configuration
        self.md = MarkdownifyTransformer(
            heading_style="ATX",  # Use # style headings
        )

    def convert_html_to_markdown(self, html_content: str) -> Optional[str]:
        """
        Convert HTML string to Markdown using markdownify.
        Handles potential errors during conversion.

        Args:
            html_content: The HTML content to convert

        Returns:
            The converted markdown content, or None if conversion fails
        """
        if not html_content:
            return None
        try:
            # Create a Document object with the HTML content
            doc = Document(page_content=html_content)
            # Transform the document with proper configuration
            transformed_doc = self.md.transform_documents([doc])

            # Get the transformed content and clean it
            return transformed_doc[0].page_content.strip()

        except Exception as e:
            self.logger.error(f"Failed to convert HTML to Markdown: {e}", exc_info=True)
            # Optionally return a placeholder or the original HTML if preferred
            return "[Content Conversion Failed]"

    def format_content(self, item: ContentItem, markdown_content: str) -> str:
        """
        Format the content with metadata header.

        Args:
            item: The ContentItem with metadata
            markdown_content: The processed markdown content

        Returns:
            Formatted content with headers
        """
        title = item.title or "No Title"
        link = item.link or "No Link"
        fetch_date = item.fetch_date or "Unknown"
        published_date = item.published_date or "Unknown"

        header = f"Date Updated: {fetch_date}\nDate Published: {published_date}\n\nTitle: {title}\n\nURL Source: {link}\n\nMarkdown Content:\n"
        return header + markdown_content

    def process_content(self, items: List[ContentItem]) -> List[ContentItem]:
        """
        Process a list of content items - converting HTML to markdown and formatting the output.
        Also determines if content is behind a paywall.

        Args:
            items: List of ContentItem objects with html_content

        Returns:
            List of processed ContentItem objects with markdown_content added
        """
        processed_items = []

        for item in items:
            html_content = item.html_content

            # Convert HTML to Markdown
            markdown_content = (
                self.convert_html_to_markdown(html_content)
                if html_content
                else "[No Content Found]"
            )

            # Format with header
            formatted_content = self.format_content(item, markdown_content)

            # Update the item with markdown content and processing results
            item.markdown_content = formatted_content

            # Check if content is paywalled/teaser using utility function
            item.is_paywall = is_paywall_or_teaser(formatted_content)

            if item.is_paywall:
                self.logger.info(f"Item {item.guid} detected as paywall/teaser")

            # Set the md_path for markdown content - indicates processed state
            item.md_path = f"markdown/{item.guid}.md"

            processed_items.append(item)

        self.logger.info(f"Processed {len(processed_items)} content items")
        return processed_items

    # Add a method for processing a single item
    def process_item(self, item: ContentItem) -> ContentItem:
        """
        Process a single content item - converting HTML to markdown and formatting the output.
        Also determines if content is behind a paywall.

        Args:
            item: ContentItem object with html_content

        Returns:
            Processed ContentItem with markdown_content added
        """
        # Simply reuse the list processing logic
        return self.process_content([item])[0]

    def _check_markdown_at_paths(self, item: ContentItem) -> bool:
        """
        Check if markdown content exists for an item.

        Args:
            item: The ContentItem to check

        Returns:
            True if markdown exists, False otherwise
        """
        if not self.s3_storage or not item.guid:
            return False

        # Define standard markdown path format - only use the current one as we'll clean up infrastructure
        path_formats = [
            "markdown/{guid}.md",  # Standard format
        ]

        # Use the centralized method in S3Storage
        return self.s3_storage.check_content_exists_at_paths(
            guid=item.guid, path_formats=path_formats, configured_path=item.md_path
        )

    def process_and_update_state(
        self,
        items_to_process: List[ContentItem],
        overwrite_flag: bool = False,
    ) -> List[ContentItem]:
        """
        Process a list of content items and update their state in S3 and DynamoDB.
        This method encapsulates the entire process stage logic.

        Args:
            items_to_process: List of ContentItem objects to process
            overwrite_flag: Whether to process items that are already processed

        Returns:
            List of processed ContentItem objects
        """
        if not self.s3_storage or not self.state_manager:
            self.logger.error(
                "S3Storage and DynamoDBState are required for process_and_update_state"
            )
            return []

        processed_items = []
        skipped_already_processed = 0
        skipped_no_html = 0
        successfully_processed = 0

        for item in items_to_process:
            # Check if markdown content already exists across possible paths
            has_markdown = self._check_markdown_at_paths(item)

            # Skip already processed items unless overwrite is enabled
            if has_markdown and not overwrite_flag:
                self.logger.info(
                    f"Item '{item.title}' ({item.guid}) already processed (has markdown content), skipping..."
                )
                processed_items.append(item)
                skipped_already_processed += 1
                continue

            # Fetch HTML content from S3 if not already loaded
            if not item.html_content and item.html_path:
                html_content = self.s3_storage.get_content(item.html_path)
                if html_content:
                    item.html_content = html_content
                else:
                    self.logger.info(
                        f"Could not retrieve HTML content for {item.guid} from {item.html_path}, skipping..."
                    )
                    skipped_no_html += 1
                    continue

            # Process the item (convert HTML to markdown)
            processed_item = self.process_item(item)

            # Check if we need to store the markdown content
            if processed_item.markdown_content:
                s3_key = item.md_path if item.md_path else f"markdown/{item.guid}.md"

                # Store markdown content in S3 if we're overwriting or it's a new file
                if self.s3_storage.store_content(
                    s3_key, processed_item.markdown_content
                ):
                    # Only set the md_path if it's not already set
                    if not processed_item.md_path:
                        processed_item.md_path = s3_key

                    # Update the item in DynamoDB - will now preserve existing fields
                    self.state_manager.update_item(processed_item, overwrite_flag)
                    self.logger.info(
                        f"Updated item '{processed_item.title}' ({processed_item.guid}): stored markdown content at {s3_key}"
                    )
                    processed_items.append(processed_item)
                    successfully_processed += 1
                else:
                    self.logger.error(
                        f"Failed to store markdown content for {processed_item.guid}"
                    )
            else:
                self.logger.info(f"No markdown content generated for {item.guid}")

        # Log summary stats
        total_skipped = skipped_already_processed + skipped_no_html
        if total_skipped > 0:
            self.logger.warning(
                f"Skipped processing {total_skipped} items: {skipped_already_processed} already processed, {skipped_no_html} missing HTML"
            )

        self.logger.info(
            f"Process summary: {successfully_processed} items processed, {total_skipped} items skipped"
        )
        return processed_items
