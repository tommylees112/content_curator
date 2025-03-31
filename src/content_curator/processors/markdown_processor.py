from typing import Any, Dict, List, Optional

from langchain_community.document_transformers import MarkdownifyTransformer
from langchain_core.documents import Document
from loguru import logger


class MarkdownProcessor:
    """Handles content processing tasks like HTML to Markdown conversion and summarization."""

    def __init__(self):
        """Initialize the content processor with necessary transformers."""
        self.logger = logger
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

    def format_content(self, item: Dict[str, Any], markdown_content: str) -> str:
        """
        Format the content with metadata header.

        Args:
            item: The content item dictionary with metadata
            markdown_content: The processed markdown content

        Returns:
            Formatted content with headers
        """
        title = item.get("title", "No Title")
        link = item.get("link", "No Link")
        fetch_date = item.get("fetch_date", "Unknown")

        header = f"Date Updated: {fetch_date}\n\nTitle: {title}\n\nURL Source: {link}\n\nMarkdown Content:\n"
        return header + markdown_content

    def process_content(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a list of content items - converting HTML to markdown and formatting the output.

        Args:
            items: List of content items with html_content

        Returns:
            List of processed items with markdown_content added
        """
        processed_items = []

        for item in items:
            html_content = item.get("html_content")

            # Convert HTML to Markdown
            markdown_content = (
                self.convert_html_to_markdown(html_content)
                if html_content
                else "[No Content Found]"
            )

            # Format with header
            formatted_content = self.format_content(item, markdown_content)

            # Create a new item with all original data plus the markdown content
            processed_item = item.copy()
            processed_item["markdown_content"] = formatted_content

            processed_items.append(processed_item)

        self.logger.info(f"Processed {len(processed_items)} content items")
        return processed_items
