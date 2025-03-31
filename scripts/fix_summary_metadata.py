import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))

from src.content_curator.processors.markdown_processor import MarkdownProcessor
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.utils import check_resources


def setup_services():
    """Initialize and check AWS services."""
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

    # Check if resources exist
    s3_exists = check_resources(s3_storage)
    dynamo_exists = check_resources(state_manager)

    if not (s3_exists and dynamo_exists):
        logger.error(
            "Required AWS resources do not exist. Please ensure your S3 bucket and DynamoDB table are created."
        )
        sys.exit(1)

    return state_manager, s3_storage


def fix_summary_metadata(dry_run=False, evaluate_content=False):
    """
    Check S3 bucket for existing summary files and update DynamoDB metadata
    to correctly mark items as summarized.

    Args:
        dry_run: If True, only report what would be updated without making changes
        evaluate_content: If True, also evaluate content quality to update to_be_summarized flag
    """
    # Load environment variables
    load_dotenv()
    logger.info("Starting summary metadata fix utility...")

    if dry_run:
        logger.info("DRY RUN MODE: No actual updates will be made")

    # Setup services
    state_manager, s3_storage = setup_services()

    # Setup processor for content evaluation if needed
    processor = MarkdownProcessor() if evaluate_content else None

    # Get all processed items from DynamoDB
    processed_items = state_manager.get_items_by_status_flags(
        is_processed=True, limit=1000
    )
    logger.info(f"Found {len(processed_items)} processed items to check")

    # Log S3 paths being used
    logger.info("Using S3 paths:")
    logger.info("- For standard summaries: processed/summaries/{guid}.md")
    logger.info("- For brief summaries: processed/short_summaries/{guid}.md")

    updated_count = 0
    content_evaluated_count = 0

    for item in processed_items:
        guid = item.get("guid")
        if not guid:
            logger.warning("Found item without guid, skipping")
            continue

        # Check if item is already marked as summarized
        is_summarized = item.get("is_summarized", False)
        to_be_summarized = item.get("to_be_summarized")

        # Set up updates dictionary
        updates = {}
        needs_update = False

        # Construct expected S3 paths - CORRECT paths with slashes
        standard_summary_path = f"processed/summaries/{guid}.md"
        short_summary_path = f"processed/short_summaries/{guid}.md"

        # Check if S3 objects exist
        standard_exists = s3_storage.object_exists(standard_summary_path)
        short_exists = s3_storage.object_exists(short_summary_path)

        # If summaries exist but metadata doesn't reflect it, mark for update
        if standard_exists and not is_summarized:
            logger.info(f"Item {guid}: Found summary in S3 but not marked in metadata")
            updates["is_summarized"] = True
            updates["summary_path"] = standard_summary_path
            needs_update = True

            if short_exists:
                updates["short_summary_path"] = short_summary_path
                logger.info(f"Item {guid}: Also found short summary in S3")

        # If evaluating content quality
        if evaluate_content and to_be_summarized is None:
            # Get the markdown content from S3
            s3_path = item.get("s3_path")
            if s3_path:
                markdown_content = s3_storage.get_content(s3_path)
                if markdown_content:
                    # Evaluate if the content is worth summarizing
                    is_paywall = processor.is_paywall_or_teaser(markdown_content)
                    worth_summarizing = (
                        False
                        if is_paywall
                        else processor.is_worth_summarizing(
                            markdown_content, min_failures_to_reject=3
                        )
                    )

                    # Add to updates
                    updates["is_paywall"] = is_paywall
                    updates["to_be_summarized"] = worth_summarizing
                    needs_update = True
                    content_evaluated_count += 1

                    if is_paywall:
                        logger.info(f"Item {guid}: Detected as paywall/teaser content")
                    elif worth_summarizing:
                        logger.info(f"Item {guid}: Content is worth summarizing")
                    else:
                        logger.info(f"Item {guid}: Content is NOT worth summarizing")

        # Update metadata if needed
        if needs_update:
            updates["last_updated"] = datetime.now().isoformat()

            if not dry_run:
                state_manager.update_metadata(guid=guid, updates=updates)
                logger.info(f"Updated item {guid} with metadata: {updates}")
            else:
                logger.info(f"[DRY RUN] Would update item {guid} with: {updates}")

            updated_count += 1

    if dry_run:
        logger.info(
            f"Completed! Found {updated_count} items that would be updated (dry run)"
        )
    else:
        logger.info(f"Completed! Updated {updated_count} items with metadata fixes")

    if evaluate_content:
        logger.info(f"Evaluated content quality for {content_evaluated_count} items")

    # Check for inconsistent S3 paths
    check_incorrect_paths(s3_storage, dry_run)


def check_incorrect_paths(s3_storage, dry_run=False):
    """
    Check for items using incorrect S3 paths (without slash).

    Args:
        s3_storage: S3Storage instance
        dry_run: If True, only report without making changes
    """
    logger.info("Checking for items using incorrect S3 paths...")

    # Check if any files exist in the incorrect paths
    incorrect_standard = s3_storage.list_objects_with_prefix("processed_summaries/")
    incorrect_short = s3_storage.list_objects_with_prefix("processed_short_summaries/")

    if incorrect_standard:
        logger.warning(
            f"Found {len(incorrect_standard)} summaries in incorrect path 'processed_summaries/'"
        )
        logger.warning("These should be in 'processed/summaries/' instead")

        for path in incorrect_standard[:5]:  # Show first 5 examples
            logger.warning(f"Example incorrect path: {path}")

    if incorrect_short:
        logger.warning(
            f"Found {len(incorrect_short)} short summaries in incorrect path 'processed_short_summaries/'"
        )
        logger.warning("These should be in 'processed/short_summaries/' instead")

        for path in incorrect_short[:5]:  # Show first 5 examples
            logger.warning(f"Example incorrect path: {path}")

    if not incorrect_standard and not incorrect_short:
        logger.info(
            "No incorrect paths found. All S3 paths are using the correct format."
        )


if __name__ == "__main__":
    # Parse command line arguments
    import argparse

    parser = argparse.ArgumentParser(
        description="Fix summary metadata in DynamoDB based on S3 content"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be updated without making changes",
    )
    parser.add_argument(
        "--evaluate-content",
        action="store_true",
        help="Evaluate content quality and update to_be_summarized flag",
    )

    args = parser.parse_args()

    fix_summary_metadata(dry_run=args.dry_run, evaluate_content=args.evaluate_content)
