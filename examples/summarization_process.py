import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))

from src.content_curator.models import ContentItem
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.summarizers.summarizer import Summarizer
from src.content_curator.utils import check_resources


def summarize_content(item: ContentItem, s3_storage, summarizer):
    """
    Generate a summary for a given content item and store it in S3.

    Args:
        item: ContentItem object to summarize
        s3_storage: S3Storage object for accessing content
        summarizer: Summarizer object to generate summaries

    Returns:
        Updated ContentItem with summary information
    """
    guid = item.guid
    md_path = item.md_path

    if not md_path:
        logger.warning(f"Item {guid} has no md_path, skipping")
        return None

    # Get content from S3
    content = s3_storage.get_content(md_path)
    if not content:
        logger.warning(f"Failed to retrieve content for {guid} at {md_path}")
        return None

    # Generate summary
    logger.info(f"Generating summary for item {guid}")
    try:
        summary = summarizer.create_summary(content)
        short_summary = summarizer.create_short_summary(content)

        # Store summaries in S3
        summary_path = f"processed/summaries/{guid}.md"
        short_summary_path = f"processed/short_summaries/{guid}.md"

        s3_storage.store_content(summary_path, summary)
        s3_storage.store_content(short_summary_path, short_summary)

        # Update item with summary information
        item.is_summarized = True
        item.summary_path = summary_path
        item.short_summary_path = short_summary_path
        item.last_updated = datetime.now().isoformat()

        return item

    except Exception as e:
        logger.error(f"Error summarizing content for {guid}: {str(e)}")
        return None


def run_summarization_process():
    """
    Process items that need summarization from DynamoDB, generate summaries,
    and update the metadata.
    """
    # Load environment variables
    load_dotenv()

    # Get AWS configuration from environment variables
    s3_bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "content-curator-bucket")
    dynamodb_table_name = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    # Initialize services
    state_manager = DynamoDBState(
        dynamodb_table_name=dynamodb_table_name, aws_region=aws_region
    )
    s3_storage = S3Storage(s3_bucket_name=s3_bucket_name, aws_region=aws_region)
    summarizer = Summarizer()

    # Check if resources exist
    s3_exists = check_resources(s3_storage)
    dynamo_exists = check_resources(state_manager)

    if not (s3_exists and dynamo_exists):
        logger.error(
            "Required AWS resources do not exist. Please ensure your S3 bucket and DynamoDB table are created."
        )
        sys.exit(1)

    # Get items that need summarization (processed, worth summarizing, not summarized yet)
    items_to_summarize = state_manager.get_items_needing_summarization(
        limit=10, as_content_items=True
    )

    if not items_to_summarize:
        logger.info("No items found that need summarization.")
        return

    logger.info(f"Found {len(items_to_summarize)} items that need summarization.")

    # Process each item
    for item in items_to_summarize:
        updated_item = summarize_content(item, s3_storage, summarizer)

        if updated_item:
            # Update item in DynamoDB
            state_manager.update_item(updated_item)
            logger.info(f"Updated metadata for item {updated_item.guid}")


if __name__ == "__main__":
    logger.info("Starting summarization process...")
    run_summarization_process()
    logger.info("Summarization process completed.")
