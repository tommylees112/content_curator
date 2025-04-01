import re
from typing import Any, Dict, List, Optional

from langchain_community.document_transformers import MarkdownifyTransformer
from langchain_core.documents import Document
from loguru import logger


class MarkdownProcessor:
    """Handles content processing tasks like HTML to Markdown conversion and summarization."""

    def __init__(self, min_content_length: int = 500):
        """
        Initialize the content processor with necessary transformers.

        Args:
            min_content_length: Minimum text length (in characters) to be considered worth summarizing
        """
        self.logger = logger
        self.min_content_length = min_content_length
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
        published_date = item.get("published_date", "Unknown")

        header = f"Date Updated: {fetch_date}\nDate Published: {published_date}\n\nTitle: {title}\n\nURL Source: {link}\n\nMarkdown Content:\n"
        return header + markdown_content

    def is_paywall_or_teaser(
        self, markdown_content: str, min_failures_to_reject: int = 2
    ) -> bool:
        """
        Detect if content appears to be behind a paywall or is just a teaser.

        Args:
            markdown_content: The markdown content to check
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

        # Check for very short content (less than 100 characters of actual text)
        if len(clean_text) < 100:
            self.logger.warning(
                f"Content detected as too short: {len(clean_text)} chars"
            )
            failed_checks += 1

        # Look for typical paywall phrases
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

        # Get text to check for paywall patterns - just the first few paragraphs
        sample_text = clean_text[:500].lower()

        # Check for paywall patterns
        found_paywall_pattern = False
        for pattern in paywall_patterns:
            if re.search(pattern, sample_text, re.IGNORECASE):
                found_paywall_pattern = True
                self.logger.info(f"Found paywall pattern: '{pattern}'")
                break

        if found_paywall_pattern:
            failed_checks += 1

        # Check link ratio
        link_ratio = len(re.findall(r"\[.*?\]\(.*?\)", content_body)) / max(
            1, len(clean_text) / 100
        )
        if link_ratio > 0.2:  # More than 1 link per 500 chars
            self.logger.info(f"Content has high link ratio: {link_ratio:.2f}")
            failed_checks += 1

        # Only mark as paywall if enough checks failed
        if failed_checks >= min_failures_to_reject:
            self.logger.info(
                f"Content marked as paywall/teaser: {failed_checks} checks failed (minimum {min_failures_to_reject} required)"
            )
            return True

        return False

    def is_worth_summarizing(
        self,
        markdown_content: str,
        min_content_length: int = None,
        max_punctuation_ratio: float = 0.05,  # 1 ! or ? per 20 chars
        min_sentences: int = 5,
        min_paragraphs: int = 3,
        min_failures_to_reject: int = 3,  # Number of checks that must fail to reject content
    ) -> bool:
        """
        Determine if content is worth summarizing based on length and quality heuristics.

        Args:
            markdown_content: The markdown content to check
            min_content_length: Minimum text length to consider summarizing (defaults to self.min_content_length)
            max_punctuation_ratio: Maximum allowed ratio of ! or ? to total characters
            min_sentences: Minimum number of sentences required
            min_paragraphs: Minimum number of paragraphs required
            min_failures_to_reject: Minimum number of quality checks that must fail to reject content

        Returns:
            True if content is worth summarizing
        """
        # Use instance min_content_length if not provided
        if min_content_length is None:
            min_content_length = self.min_content_length

        # Skip paywall/teaser content - this is an automatic rejection
        if self.is_paywall_or_teaser(markdown_content):
            self.logger.info("Content is behind paywall or just a teaser, skipping")
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
            self.logger.info(
                f"Content too short for summarization: {len(clean_text)} chars"
            )
            failed_checks += 1

        # Check for excessive punctuation or unusual patterns
        punct_count = len(re.findall(r"[!?]", clean_text))
        punct_ratio = punct_count / max(1, len(clean_text))
        if punct_ratio > max_punctuation_ratio:
            self.logger.info(
                f"Content has excessive punctuation: {punct_count} out of {len(clean_text)} chars ({punct_ratio:.4f})"
            )
            failed_checks += 1

        # Count sentences as a rough proxy for article development
        sentences = re.split(r"[.!?]+", clean_text)
        if len(sentences) < min_sentences:
            self.logger.info(
                f"Content has too few sentences: {len(sentences)} < {min_sentences}"
            )
            failed_checks += 1

        # Count paragraphs
        paragraphs = [p for p in content_body.split("\n\n") if p.strip()]
        if len(paragraphs) < min_paragraphs:
            self.logger.info(
                f"Content has too few paragraphs: {len(paragraphs)} < {min_paragraphs}"
            )
            failed_checks += 1

        # Check if enough checks failed to reject the content
        if failed_checks >= min_failures_to_reject:
            self.logger.info(
                f"Content failed {failed_checks} quality checks, minimum {min_failures_to_reject} required to reject"
            )
            return False

        return True

    def process_content(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a list of content items - converting HTML to markdown and formatting the output.
        Also determines if content is behind a paywall and if it's worth summarizing.

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

            # Check if content is paywalled/teaser
            is_paywall = self.is_paywall_or_teaser(formatted_content)
            processed_item["is_paywall"] = is_paywall

            # Check if content is worth summarizing
            to_be_summarized = (
                False
                if is_paywall
                else self.is_worth_summarizing(
                    formatted_content,
                    min_failures_to_reject=3,  # Require at least 3 failures to reject
                )
            )
            processed_item["to_be_summarized"] = to_be_summarized

            if is_paywall:
                self.logger.info(
                    f"Item {item.get('guid', 'unknown')} detected as paywall/teaser"
                )
            if to_be_summarized:
                self.logger.info(
                    f"Item {item.get('guid', 'unknown')} marked for summarization"
                )
            else:
                self.logger.info(
                    f"Item {item.get('guid', 'unknown')} will not be summarized"
                )

            processed_items.append(processed_item)

        self.logger.info(f"Processed {len(processed_items)} content items")
        return processed_items
