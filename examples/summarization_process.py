import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from src.content_curator.aws_storage import AwsStorage

sys.path.append(str(Path(__file__).parent.parent))


def summarize_content(content: str) -> str:
    """
    Example function that would summarize the content.
    Replace with your actual summarization logic.
    """
    # This is just a placeholder - implement your actual summarization here
    summary = f"## Summary\n\nThis is a summary of: {content[:100]}...\n\nGenerated on: {datetime.now().isoformat()}"
    return summary


def run_summarization_process():
    """
    Example process that summarizes content items that haven't been summarized yet.
    """
    # Load environment variables
    load_dotenv()

    # Get AWS configuration from environment variables
    s3_bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "content-curator-bucket")
    dynamodb_table_name = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    # Initialize AWS storage
    aws_storage = AwsStorage(
        s3_bucket_name=s3_bucket_name,
        dynamodb_table_name=dynamodb_table_name,
        aws_region=aws_region,
    )

    # Check if AWS resources exist
    if not aws_storage.check_resources_exist():
        logger.error(
            "Required AWS resources do not exist. Please check your configuration."
        )
        return

    # Method 1: Get all items that need summarization (status = 'fetched')
    items_to_summarize = aws_storage.get_items_needing_processing(
        target_status="fetched", limit=10
    )

    logger.info(f"Found {len(items_to_summarize)} items that need summarization")

    # Process each item
    summarized_count = 0
    for item in items_to_summarize:
        guid = item.get("guid")
        s3_path = item.get("s3_path")

        # Double-check the item still needs processing (to handle concurrent processing)
        if not aws_storage.needs_summarization(guid):
            logger.info(
                f"Item {guid} was already summarized by another process, skipping"
            )
            continue

        # Get the content from S3
        content = aws_storage.get_content_from_s3(s3_path)
        if not content:
            logger.error(f"Could not retrieve content for item {guid}, skipping")
            continue

        # Generate summary
        summary = summarize_content(content)

        # Store the summary and update status
        summary_path = aws_storage.store_processed_summary(guid, summary)
        if summary_path:
            summarized_count += 1

    logger.info(f"Successfully summarized {summarized_count} items")

    # Method 2: Alternative approach - process a list of specific GUIDs
    guids_to_process = ["example-guid-1", "example-guid-2", "example-guid-3"]

    for guid in guids_to_process:
        # Check if the item exists and needs summarization
        if not aws_storage.needs_summarization(guid):
            logger.info(
                f"Item {guid} doesn't exist or doesn't need summarization, skipping"
            )
            continue

        # Get the metadata
        metadata = aws_storage.get_item_metadata(guid)
        if not metadata:
            logger.error(f"Could not retrieve metadata for item {guid}, skipping")
            continue

        # Get the content
        s3_path = metadata.get("s3_path")
        content = aws_storage.get_content_from_s3(s3_path)
        if not content:
            logger.error(f"Could not retrieve content for item {guid}, skipping")
            continue

        # Generate and store the summary
        summary = summarize_content(content)
        aws_storage.store_processed_summary(guid, summary)


if __name__ == "__main__":
    run_summarization_process()
