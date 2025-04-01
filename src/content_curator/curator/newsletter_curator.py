from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from loguru import logger

from src.content_curator.models import ContentItem, SummaryType
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.utils import parse_date


class NewsletterCurator:
    """
    Handles curation of newsletter content by combining the most recent short summaries.
    """

    def __init__(
        self, state_manager: DynamoDBState, s3_storage: Optional[S3Storage] = None
    ):
        """
        Initialize the newsletter curator.

        Args:
            state_manager: The DynamoDB state manager
            s3_storage: Optional S3 storage for fetching content
        """
        self.state_manager = state_manager
        self.s3_storage = s3_storage
        self.logger = logger

    def get_recent_content(
        self,
        most_recent: Optional[int] = None,
        n_days: Optional[int] = None,
        summary_type: SummaryType = "brief",
    ) -> List[ContentItem]:
        """
        Get recent content summaries based on either the most recent n items or the items
        from the last n days.

        Args:
            most_recent: Number of most recent items to get
            n_days: Number of days to look back from today
            summary_type: Type of summary to retrieve ("brief" for short_summary_path,
                        "standard" for summary_path)

        Returns:
            List of ContentItem objects with summaries
        """
        if most_recent is None and n_days is None:
            self.logger.error("Either most_recent or n_days must be provided")
            return []

        # Get all items with summaries based on path existence
        path_field = "short_summary_path" if summary_type == "brief" else "summary_path"

        # Use the path-based query method
        all_items = self.state_manager.get_items_by_status_paths(
            summary_path_exists=True if path_field == "summary_path" else None,
            as_content_items=True,
        )

        # Filter for items that have the requested summary type
        summarized_items = [
            item for item in all_items if getattr(item, path_field, None)
        ]

        # Calculate the cutoff date if needed
        cutoff_date = None
        if n_days is not None:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=n_days)

        # Process items: parse dates and filter by cutoff date if needed
        processed_items = []
        for item in summarized_items:
            # Get and parse the publication date
            pub_date_str = item.published_date
            if not pub_date_str:
                # If no publication date, it can't be filtered by date,
                # but we'll still include it for the most_recent filter
                if n_days is None:
                    processed_items.append((None, item))
                continue

            # Parse the date string to a datetime object
            pub_date = parse_date(pub_date_str)

            # Add to processed items if it meets the date criteria or if we're not filtering by date
            if n_days is None or (pub_date and pub_date >= cutoff_date):
                processed_items.append((pub_date, item))

        # Sort by publication date (newest first)
        # Items with no date will be at the end
        processed_items.sort(key=lambda x: x[0] if x[0] else datetime.min, reverse=True)

        # Extract just the items from the sorted list
        result = [item for _, item in processed_items]

        # Apply the most_recent filter if specified
        if most_recent is not None:
            result = result[:most_recent]
            self.logger.info(f"Retrieved {len(result)} most recent items")
        else:
            self.logger.info(
                f"Retrieved {len(result)} items from the last {n_days} days"
            )

        return result

    def format_recent_content(
        self,
        items: List[ContentItem],
        summary_type: SummaryType = "brief",
    ) -> str:
        """
        Format the recent content items in the specified format.

        <title>
        <date published>
        <url>
        <summary>

        Args:
            items: List of ContentItem objects with summaries
            summary_type: Type of summary to use in formatting

        Returns:
            Formatted content as string
        """
        if not items:
            return "No recent content available."

        formatted_content = "## Recent Content\n\n"

        # Determine which attributes to use based on summary_type
        summary_attr = "short_summary" if summary_type == "brief" else "summary"
        path_attr = "short_summary_path" if summary_type == "brief" else "summary_path"

        for item in items:
            title = item.title or "Untitled"
            url = item.link or ""
            published_date = item.published_date or ""

            # Format date if present
            date_str = f"Published: {published_date}\n" if published_date else ""

            # Get the summary content - first check if it's already in the item
            summary = getattr(item, summary_attr, None)

            # If no summary and we have S3 storage, try to get it from S3
            if (not summary or summary == "No summary available") and self.s3_storage:
                content_path = getattr(item, path_attr, None)
                if content_path:
                    # Get summary content from S3
                    s3_content = self.s3_storage.get_content(content_path)
                    if s3_content:
                        summary = s3_content
                        # Cache it in the item
                        setattr(item, summary_attr, s3_content)

            # If still no summary, use a placeholder
            if not summary:
                if self.s3_storage:
                    summary = f"Could not retrieve summary from: {getattr(item, path_attr, 'No path available')}"
                else:
                    summary = f"Summary located at: {getattr(item, path_attr, 'No path available')}"

            # Format each item
            formatted_content += f"### {title}\n"
            formatted_content += f"{date_str}\n"
            formatted_content += f"{url}\n"
            formatted_content += f"{summary}\n\n"

        return formatted_content

    def curate_recent_content(
        self,
        most_recent: Optional[int] = None,
        n_days: Optional[int] = None,
        summary_type: SummaryType = "brief",
    ) -> Tuple[str, List[str]]:
        """
        Get and format recent content for a newsletter.

        Args:
            most_recent: Number of most recent items to include
            n_days: Number of days to look back from today
            summary_type: Type of summary to use ("brief" or "standard")

        Returns:
            Tuple containing:
                - Formatted content as string
                - List of item GUIDs included in the newsletter
        """
        # Get recent content
        recent_items = self.get_recent_content(
            most_recent=most_recent, n_days=n_days, summary_type=summary_type
        )

        # Format the content
        formatted_content = self.format_recent_content(
            items=recent_items, summary_type=summary_type
        )

        # Extract the item GUIDs for tracking newsletter inclusion
        included_guids = [item.guid for item in recent_items if item.guid]

        # Update newsletters list for each item to indicate distribution
        newsletter_id = f"newsletter_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        for item in recent_items:
            if not hasattr(item, "newsletters") or item.newsletters is None:
                item.newsletters = []
            item.newsletters.append(newsletter_id)
            # Update item in DynamoDB
            self.state_manager.update_item(item)

        return formatted_content, included_guids

    def curate_and_update_state(
        self,
        most_recent: Optional[int] = 5,
        n_days: Optional[int] = None,
        summary_type: SummaryType = "brief",
    ) -> str:
        """
        Generate a newsletter, save it to S3, and update DynamoDB state.
        This method encapsulates the entire curation stage logic.

        Args:
            most_recent: Number of most recent items to include
            n_days: Number of days to look back from today
            summary_type: Type of summary to use ("brief" or "standard")

        Returns:
            The generated newsletter content
        """
        if not self.s3_storage:
            self.logger.error("S3Storage is required for curate_and_update_state")
            return ""

        # Get and format recent content
        curated_content, included_items = self.curate_recent_content(
            most_recent=most_recent, n_days=n_days, summary_type=summary_type
        )

        if not curated_content:
            self.logger.warning("No content available for newsletter curation.")
            return ""

        # Create a timestamp for the filename
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        newsletter_id = f"newsletter_{timestamp}"

        # Save the curated content to S3 with timestamp
        s3_key = f"curated/{newsletter_id}.md"
        if self.s3_storage.store_content(s3_key, curated_content):
            self.logger.info(f"Newsletter saved to S3 at {s3_key}")
        else:
            self.logger.error("Failed to save newsletter to S3")

        # Also save at a type-specific location as "latest_{summary_type}.md"
        latest_key = f"curated/latest_{summary_type}.md"
        if self.s3_storage.store_content(latest_key, curated_content):
            self.logger.info(
                f"Newsletter saved to S3 at {latest_key} (latest {summary_type} version)"
            )
        else:
            self.logger.error(f"Failed to save latest {summary_type} newsletter to S3")

        return curated_content


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    dynamodb_table_name: str = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket_name: str = os.getenv("AWS_S3_BUCKET_NAME", "content-curator")

    # Initialize services
    state_manager = DynamoDBState(
        dynamodb_table_name=dynamodb_table_name, aws_region=aws_region
    )
    s3_storage = S3Storage(s3_bucket_name=s3_bucket_name, aws_region=aws_region)

    # Create the curator with both services
    curator = NewsletterCurator(state_manager=state_manager, s3_storage=s3_storage)

    # Get short summaries (most recent 5)
    print("SHORT SUMMARIES (MOST RECENT 5):")
    # content, included_items = curator.curate_recent_content(
    #     n_days=3, summary_type="brief"
    # )
    content, included_items = curator.curate_recent_content(
        most_recent=5, summary_type="brief"
    )

    print(content)
    print("\n" + "=" * 80 + "\n")

    # # Get standard summaries (most recent 5)
    # print("STANDARD SUMMARIES (MOST RECENT 3):")
    # print(curator.curate_recent_content(n_days=3, summary_type="standard"))
