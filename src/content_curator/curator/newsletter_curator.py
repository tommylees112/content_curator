from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional

from loguru import logger

from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage


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
        summary_type: Literal["short", "standard"] = "short",
    ) -> List[Dict[str, Any]]:
        """
        Get recent content summaries based on either the most recent n items or the items
        from the last n days.

        Args:
            most_recent: Number of most recent items to get
            n_days: Number of days to look back from today
            summary_type: Type of summary to retrieve ("short" for short_summary_path,
                        "standard" for summary_path)

        Returns:
            List of items with summaries
        """
        if most_recent is None and n_days is None:
            self.logger.error("Either most_recent or n_days must be provided")
            return []

        # Get all items with summaries
        all_items = self.state_manager.get_all_items()

        # Determine which path field to check based on summary_type
        path_field = "short_summary_path" if summary_type == "short" else "summary_path"

        # Filter for items that have been summarized with the requested summary type
        summarized_items = [
            item
            for item in all_items
            if item.get(path_field) and item.get("is_summarized", False)
        ]

        # Sort items by published_date in descending order (newest first)
        # If published_date is not available, fall back to timestamp
        summarized_items.sort(
            key=lambda x: x.get("published_date", x.get("timestamp", "")), reverse=True
        )

        if not summarized_items:
            self.logger.warning(f"No items with {summary_type} summaries found")
            return []

        # Apply filtering based on the criteria
        if most_recent is not None:
            result = summarized_items[:most_recent]
            self.logger.info(f"Retrieved {len(result)} most recent items")
            return result

        elif n_days is not None:
            # Calculate the cutoff date as a datetime object
            cutoff_date = datetime.now() - timedelta(days=n_days)

            # Filter items newer than the cutoff date
            result = []
            for item in summarized_items:
                pub_date_str = item.get("published_date", "")
                if not pub_date_str:
                    continue

                try:
                    # Try to parse the date string to a datetime object
                    # Handle different formats that might be present
                    if "T" in pub_date_str:
                        # ISO format like "2023-06-22T13:44:50"
                        pub_date = datetime.fromisoformat(
                            pub_date_str.replace("Z", "+00:00")
                        )
                    elif "GMT" in pub_date_str:
                        # Format like "Wed, 22 Jun 2023 13:44:50 GMT"
                        pub_date_str = pub_date_str.replace("GMT", "+0000")
                        pub_date = datetime.strptime(
                            pub_date_str, "%a, %d %b %Y %H:%M:%S %z"
                        )
                    else:
                        # Try a simple format as fallback
                        pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d %H:%M:%S")

                    # Compare as datetime objects
                    if pub_date >= cutoff_date:
                        result.append(item)
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Could not parse date {pub_date_str}: {e}")
                    continue

            self.logger.info(
                f"Retrieved {len(result)} items from the last {n_days} days"
            )
            return result

    def format_recent_content(
        self,
        items: List[Dict[str, Any]],
        summary_type: Literal["short", "standard"] = "short",
    ) -> str:
        """
        Format the recent content items in the specified format.

        <title>
        <date published>
        <url>
        <summary>

        Args:
            items: List of content items with summaries
            summary_type: Type of summary to use in formatting

        Returns:
            Formatted content as string
        """
        if not items:
            return "No recent content available."

        formatted_content = "## Recent Content\n\n"

        # Determine which fields to use based on summary_type
        summary_field = "short_summary" if summary_type == "short" else "summary"
        path_field = "short_summary_path" if summary_type == "short" else "summary_path"

        for item in items:
            title = item.get("title", "Untitled")
            url = item.get("link", "")
            published_date = item.get("published_date", "")

            # Format date if present
            date_str = f"Published: {published_date}\n" if published_date else ""

            # Get the summary content - first check if it's already in the item
            summary = item.get(summary_field)

            # If no summary and we have S3 storage, try to get it from S3
            if (not summary or summary == "No summary available") and self.s3_storage:
                s3_path = item.get(path_field)
                if s3_path:
                    # Get summary content from S3
                    s3_content = self.s3_storage.get_content(s3_path)
                    if s3_content:
                        summary = s3_content
                        # Cache it in the item
                        item[summary_field] = s3_content

            # If still no summary, use a placeholder
            if not summary:
                if self.s3_storage:
                    summary = f"Could not retrieve summary from: {item.get(path_field, 'No path available')}"
                else:
                    summary = f"Summary located at: {item.get(path_field, 'No path available')}"

            # Format each item
            formatted_content += f"### {title}\n"
            formatted_content += date_str
            formatted_content += f"{url}\n"
            formatted_content += f"{summary}\n\n"

        return formatted_content

    def curate_recent_content(
        self,
        most_recent: Optional[int] = None,
        n_days: Optional[int] = None,
        summary_type: Literal["short", "standard"] = "short",
    ) -> str:
        """
        Get and format recent content for a newsletter.

        Args:
            most_recent: Number of most recent items to include
            n_days: Number of days to look back from today
            summary_type: Type of summary to use ("short" or "standard")

        Returns:
            Formatted content as string
        """
        # Get recent content
        recent_items = self.get_recent_content(
            most_recent=most_recent, n_days=n_days, summary_type=summary_type
        )

        # Format the content
        formatted_content = self.format_recent_content(
            items=recent_items, summary_type=summary_type
        )

        return formatted_content


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    dynamodb_table_name: str = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket_name: str = os.getenv("AWS_S3_BUCKET_NAME", "content-curator-bucket")

    # Initialize services
    state_manager = DynamoDBState(
        dynamodb_table_name=dynamodb_table_name, aws_region=aws_region
    )
    s3_storage = S3Storage(s3_bucket_name=s3_bucket_name, aws_region=aws_region)

    # Create the curator with both services
    curator = NewsletterCurator(state_manager=state_manager, s3_storage=s3_storage)

    # Get short summaries (most recent 5)
    print("SHORT SUMMARIES (MOST RECENT 5):")
    print(curator.curate_recent_content(n_days=3, summary_type="short"))
    print("\n" + "=" * 80 + "\n")

    # # Get standard summaries (most recent 5)
    # print("STANDARD SUMMARIES (MOST RECENT 3):")
    # print(curator.curate_recent_content(n_days=3, summary_type="standard"))
