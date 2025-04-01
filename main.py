import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger

from src.content_curator.curator.newsletter_curator import NewsletterCurator
from src.content_curator.fetchers.rss_fetcher import RssFetcher
from src.content_curator.models import ContentItem
from src.content_curator.processors.markdown_processor import MarkdownProcessor
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.summarizers.summarizer import Summarizer
from src.content_curator.utils import check_resources

log_file = "content_curator.log"
logger.add(
    log_file,
    rotation="10 MB",
    retention=5,
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
)


def parse_arguments():
    """Parse command line arguments to control pipeline stages."""
    parser = argparse.ArgumentParser(description="Content Curator Pipeline")

    # Add arguments for each pipeline stage
    parser.add_argument(
        "--fetch", action="store_true", help="Run the fetch stage to get new content"
    )
    parser.add_argument(
        "--process",
        action="store_true",
        help="Run the processing stage to convert HTML to markdown",
    )
    parser.add_argument(
        "--summarize",
        action="store_true",
        help="Run the summarization stage to generate summaries",
    )
    parser.add_argument(
        "--curate",
        action="store_true",
        help="Run the curation stage to create newsletters",
    )
    parser.add_argument("--all", action="store_true", help="Run all pipeline stages")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing processed or summarized content",
    )
    parser.add_argument(
        "--save_locally",
        action="store_true",
        help="Save processed content and newsletters to local files for debugging",
    )
    parser.add_argument(
        "--id",
        type=str,
        help="Process a specific item by its ID (URL hash). If provided, will only process this item.",
    )
    parser.add_argument(
        "--rss_url",
        type=str,
        help="Process all items from a specific RSS feed URL. Uses the fetch stage to get content from this feed.",
    )
    parser.add_argument(
        "--fetch_max_items",
        type=int,
        default=5,
        help="Maximum number of most recent items to fetch per feed (default: 5). Use 0 for no limit.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # If RSS URL is provided, enable appropriate stages
    if args.rss_url:
        # When an RSS URL is provided, enable all stages by default unless specific stages are requested
        if not (args.fetch or args.process or args.summarize or args.curate):
            args.fetch = True
            args.process = True
            args.summarize = True
            logger.info(
                "All pipeline stages enabled automatically for RSS URL processing"
            )
        # Otherwise, make sure at least fetch is enabled
        elif not args.fetch:
            args.fetch = True
            logger.info("Fetch stage enabled automatically for RSS URL processing")

    # If no arguments provided or --all specified, run all stages
    elif not (args.fetch or args.process or args.summarize or args.curate) or args.all:
        args.fetch = True
        args.process = True
        args.summarize = True
        args.curate = True

    return args


def setup_services() -> Tuple[DynamoDBState, S3Storage]:
    """Initialize and check AWS services."""
    # Get AWS configuration from environment variables
    s3_bucket_name: str = os.getenv("AWS_S3_BUCKET_NAME", "content-curator-bucket")
    dynamodb_table_name: str = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")

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


def run_fetch_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    specific_id: Optional[str] = None,
    rss_url: Optional[str] = None,
    fetch_max_items: Optional[int] = 5,
) -> List[ContentItem]:
    """Run the fetch stage to get new content."""
    # Initialize fetcher with either a file of URLs or a specific RSS URL
    if rss_url:
        # Create RssFetcher with the specific RSS URL
        logger.info(f"Fetching from RSS URL: {rss_url}")
        fetcher = RssFetcher(
            max_items=fetch_max_items,
            specific_url=rss_url,
            s3_storage=s3_storage,
            state_manager=state_manager,
        )
    else:
        # Use the default file of RSS URLs
        rss_url_file: Path = Path(__file__).parent / "data" / "rss_urls.txt"
        fetcher = RssFetcher(
            url_file_path=str(rss_url_file),
            max_items=fetch_max_items,
            s3_storage=s3_storage,
            state_manager=state_manager,
        )

    logger.info("Fetching content...")
    return fetcher.fetch_and_update_state(specific_id)


def run_process_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    fetched_items: List[ContentItem],
    fetch_flag: bool,
    overwrite_flag: bool,
    specific_id: Optional[str] = None,
) -> List[ContentItem]:
    """Run the processing stage to convert HTML to markdown."""
    # Initialize processor
    processor: MarkdownProcessor = MarkdownProcessor(
        s3_storage=s3_storage,
        state_manager=state_manager,
    )

    # If we got items from fetch stage, use those
    if fetch_flag and fetched_items:
        items_to_process = fetched_items
    else:
        # Otherwise, get items from the database using the helper method
        items_to_process = state_manager.get_items_for_stage(
            stage="process",
            specific_id=specific_id,
            overwrite_flag=overwrite_flag,
        )
        if not items_to_process:
            logger.warning("No items found for processing.")
            return []

    logger.info(f"--- Loaded {len(items_to_process)} items for processing ---")
    logger.info("Processing content...")

    # Process items and update state
    return processor.process_and_update_state(items_to_process, overwrite_flag)


def run_summarize_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    processed_items: List[ContentItem],
    process_flag: bool,
    overwrite_flag: bool,
    specific_id: Optional[str] = None,
) -> List[ContentItem]:
    """Run the summarization stage to generate summaries."""
    # Initialize summarizer
    logger.info("Summarizing content...")
    summarizer = Summarizer(
        model_name="gemini-1.5-flash",
        s3_storage=s3_storage,
        state_manager=state_manager,
    )

    # If we got items from process stage, use those
    if process_flag and processed_items:
        items_to_summarize = processed_items
    else:
        # Otherwise, get items from the database using the helper method
        items_to_summarize = state_manager.get_items_for_stage(
            stage="summarize",
            specific_id=specific_id,
            overwrite_flag=overwrite_flag,
        )
        if not items_to_summarize:
            logger.warning("No items found for summarization.")
            return []

    logger.info(f"--- Loaded {len(items_to_summarize)} items for summarization ---")

    # Summarize items and update state
    return summarizer.summarize_and_update_state(items_to_summarize, overwrite_flag)


def run_curate_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    most_recent: Optional[int] = 5,
    n_days: Optional[int] = None,
    summary_type: str = "short",
) -> str:
    """Run the curation stage to create newsletters and save them to S3."""
    logger.info("Creating newsletter from recent content...")

    # Initialize the newsletter curator
    curator = NewsletterCurator(state_manager=state_manager, s3_storage=s3_storage)

    # Generate newsletter and save to S3
    return curator.curate_and_update_state(
        most_recent=most_recent, n_days=n_days, summary_type=summary_type
    )


def save_last_item(processed_items: List[ContentItem], summarize_flag: bool):
    """Save the last processed item's content to a local file. For testing and debugging."""
    if not processed_items:
        return

    try:
        last_item = processed_items[-1]
        markdown_content = last_item.markdown_content or ""
        output_path = "/tmp/last_processed_item.md"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        logger.info(f"Saved last item's markdown content to {output_path}")

        if summarize_flag and last_item.summary:
            summary_path = "/tmp/last_item_summary.md"
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(last_item.summary)
            logger.info(f"Saved last item's summary to {summary_path}")
    except Exception as e:
        logger.error(f"Error saving markdown content: {e}")


def main():
    """Main entry point for the content curation pipeline."""
    logger.info(f"{'-' * 80}\n\n main.py execution started\n\n")
    # Load environment variables
    load_dotenv()

    # # Set up logging to file
    # log_file = setup_logging(log_level=os.getenv("LOG_LEVEL", "INFO"))

    # Parse command line arguments
    args = parse_arguments()

    # Log the arguments
    logger.info(f"Command arguments: {vars(args)}")

    # Initialize services
    state_manager, s3_storage = setup_services()

    # Initialize variables to track items through pipeline
    fetched_items: List[ContentItem] = []
    processed_items: List[ContentItem] = []
    summarized_items: List[ContentItem] = []

    # Run fetch stage if enabled
    if args.fetch:
        logger.info("\n\nRunning fetch stage...\n\n".upper())
        fetched_items = run_fetch_stage(
            state_manager, s3_storage, args.id, args.rss_url, args.fetch_max_items
        )

    # Run process stage if enabled
    if args.process:
        logger.info("\n\nRunning process stage...\n\n".upper())
        processed_items = run_process_stage(
            state_manager,
            s3_storage,
            fetched_items,
            args.fetch,
            args.overwrite,
            args.id,
        )
        # Save last processed item if requested
        if args.save_locally:
            save_last_item(processed_items, args.summarize)

    # Run summarize stage if enabled
    if args.summarize:
        logger.info("\n\nRunning summarize stage...\n\n".upper())
        summarized_items = run_summarize_stage(
            state_manager,
            s3_storage,
            processed_items,
            args.process,
            args.overwrite,
            args.id,
        )
        # Save last summarized item if requested (and not already saved in process stage)
        if args.save_locally and not args.process:
            save_last_item(summarized_items, args.summarize)

    # Run curate stage if enabled
    if args.curate:
        logger.info("\n\nRunning curate stage...\n\n".upper())
        curated_content = run_curate_stage(
            state_manager,
            s3_storage,
            most_recent=10,  # Default to 10 most recent items
        )
        # Optionally save to local file for debugging
        if curated_content and args.save_locally:
            try:
                output_path = "/tmp/latest_newsletter.md"
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(curated_content)
                logger.info(f"Saved latest newsletter to {output_path}")
            except Exception as e:
                logger.error(f"Error saving newsletter content: {e}")

    logger.info(f"Pipeline completed. Log file: {log_file}")


if __name__ == "__main__":
    main()
