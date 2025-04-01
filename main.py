import argparse
import os
import sys
from datetime import datetime
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
    # If specific_id is provided, try to fetch just that item from the database
    if specific_id and not rss_url:
        item = state_manager.get_item(specific_id)
        if item:
            return [item]
        else:
            logger.warning(f"No item found with ID: {specific_id}")
            return []

    # Convert 0 or negative values to None (no limit)
    max_items = None if fetch_max_items <= 0 else fetch_max_items

    # Initialize fetcher with either a file of URLs or a specific RSS URL
    if rss_url:
        # Create RssFetcher with the specific RSS URL
        logger.info(f"Fetching from RSS URL: {rss_url}")
        fetcher = RssFetcher(
            max_items=max_items, specific_url=rss_url, s3_storage=s3_storage
        )
    else:
        # Use the default file of RSS URLs
        rss_url_file: Path = Path(__file__).parent / "data" / "rss_urls.txt"
        fetcher = RssFetcher(
            url_file_path=str(rss_url_file), max_items=max_items, s3_storage=s3_storage
        )

    logger.info("Fetching content...")
    fetched_items: List[ContentItem] = fetcher.fetch_items()

    if not fetched_items:
        logger.warning("No items were fetched.")
        return []

    logger.info(f"--- Fetched {len(fetched_items)} items ---")

    # Store items to AWS
    for item in fetched_items:
        # Check if item already exists
        if state_manager.item_exists(item.guid):
            # Update existing item
            existing_item = state_manager.get_item(item.guid)
            if existing_item:
                # Update only fetch-related fields
                existing_item.title = item.title
                existing_item.link = item.link
                existing_item.published_date = item.published_date
                existing_item.fetch_date = item.fetch_date
                existing_item.source_url = item.source_url
                existing_item.is_fetched = True
                existing_item.html_path = item.html_path
                existing_item.last_updated = datetime.now().isoformat()

                # Update in DynamoDB
                state_manager.update_item(existing_item)
                logger.info(f"Updated fetch metadata for item: {item.guid}")
        else:
            # Store new item in DynamoDB
            item.is_fetched = True
            state_manager.store_item(item)
            logger.info(f"Created new item metadata: {item.guid} - '{item.title}'")

    return fetched_items


def run_process_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    fetched_items: List[ContentItem],
    fetch_flag: bool,
    overwrite_flag: bool,
    specific_id: Optional[str] = None,
) -> List[ContentItem]:
    """Run the processing stage to convert HTML to markdown."""
    # ALWAYS check specific_id first, regardless of other flags
    if specific_id:
        item = state_manager.get_item(specific_id)
        if item:
            fetched_items = [item]
            logger.info(f"Processing single item with ID: {specific_id}")
        else:
            logger.warning(f"No item found with ID: {specific_id}")
            return []
    # Only do bulk fetching if no specific ID was provided and we didn't get items from the fetch stage
    elif not fetch_flag or not fetched_items:
        logger.info("Loading items for processing from database...")
        if overwrite_flag:
            # If overwrite is enabled, get all fetched items regardless of processing status
            logger.info("Overwrite flag enabled - getting all fetched items...")
            fetched_items = state_manager.get_items_by_status_flags(
                is_fetched=True, as_content_items=True
            )
            more_items = state_manager.get_items_by_status_flags(
                is_fetched=False, as_content_items=True
            )
            fetched_items.extend(more_items)
        else:
            # Otherwise, get only items that need processing
            fetched_items = state_manager.get_items_by_status_flags(
                is_fetched=True, is_processed=False, as_content_items=True
            )

    if not fetched_items:
        logger.warning("No items found for processing.")
        return []

    logger.info(f"--- Loaded {len(fetched_items)} items for processing ---")

    # Get HTML content from S3 for each item if needed
    for item in fetched_items:
        if not item.html_content and item.html_path:
            html_content = s3_storage.get_content(item.html_path)
            if html_content:
                item.html_content = html_content

    # Initialize processor
    processor: MarkdownProcessor = MarkdownProcessor()

    logger.info("Processing content...")
    processed_items: List[ContentItem] = processor.process_content(fetched_items)

    # Store processed content to AWS
    logger.info("Storing processed content to AWS...")
    for item in processed_items:
        # Store markdown content in S3
        if item.markdown_content:
            s3_key = f"markdown/{item.guid}.md"
            if s3_storage.store_content(s3_key, item.markdown_content):
                # Update the item in DynamoDB
                state_manager.update_item(item)
                logger.info(
                    f"Updated item '{item.title}' ({item.guid}): marked as processed, added S3 path"
                )

    return processed_items


def run_summarize_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    processed_items: List[ContentItem],
    process_flag: bool,
    overwrite_flag: bool,
    specific_id: Optional[str] = None,
) -> List[ContentItem]:
    """Run the summarization stage to generate summaries."""
    # ALWAYS check specific_id first, regardless of other flags
    if specific_id:
        item = state_manager.get_item(specific_id)
        if item:
            # Get markdown content from S3 for the specific item
            if item.md_path:
                markdown_content = s3_storage.get_content(item.md_path)
                if markdown_content:
                    item.markdown_content = markdown_content
                    processed_items = [item]
                    logger.info(f"Processing single item with ID: {specific_id}")
                else:
                    logger.warning(f"Could not retrieve content for item {specific_id}")
                    return []
            else:
                logger.warning(f"No S3 path found for item {specific_id}")
                return []
        else:
            logger.warning(f"No item found with ID: {specific_id}")
            return []

    # Only do bulk loading if no specific ID was provided
    if not specific_id:
        # If we didn't process items or processing was disabled, get processed items from DynamoDB
        if not process_flag or not processed_items:
            logger.info("Loading items that have been processed for summarization...")
            if overwrite_flag:
                # If overwrite is enabled, get all processed items regardless of summarization status
                logger.info("Overwrite flag enabled - getting all processed items...")
                db_processed_items = state_manager.get_items_by_status_flags(
                    is_processed=True,
                    as_content_items=True,
                    limit=100,
                )
            else:
                # Otherwise, get only items that need summarization
                db_processed_items = state_manager.get_items_by_status_flags(
                    is_processed=True,
                    is_summarized=False,
                    as_content_items=True,
                    limit=100,
                )

            if not db_processed_items:
                logger.warning("No items found for summarization.")
                return []

            logger.info(
                f"--- Loaded {len(db_processed_items)} items for summarization ---"
            )

            # We need to fetch the actual content for these items
            processed_items = []
            for item in db_processed_items:
                if item.md_path:
                    # Get markdown content from S3
                    markdown_content = s3_storage.get_content(item.md_path)
                    if markdown_content:
                        item.markdown_content = markdown_content
                        processed_items.append(item)
                    else:
                        logger.warning(f"Could not retrieve content for {item.guid}")

    if not processed_items:
        logger.warning("No items with content available for summarization.")
        return []

    # Initialize summarizer
    logger.info("Summarizing content...")
    summarizer = Summarizer()

    # Process each item individually
    summarized_items = []
    items_to_summarize = 0
    items_skipped = 0

    for item in processed_items:
        # Check if item is already summarized and we're not in overwrite mode
        if not overwrite_flag and item.is_summarized:
            logger.info(
                f"Item '{item.title}' ({item.guid}) already summarized, skipping..."
            )
            summarized_items.append(item)
            continue

        # Check if this item should be summarized
        if item.to_be_summarized is None and item.markdown_content:
            processor = MarkdownProcessor()
            item.to_be_summarized = processor.is_worth_summarizing(
                item.markdown_content
            )

            # If the item is determined not to be worth summarizing, update it
            if not item.to_be_summarized:
                item.is_paywall = processor.is_paywall_or_teaser(item.markdown_content)
                state_manager.update_item(item)

        # Skip items not worth summarizing
        if not item.to_be_summarized:
            logger.warning(
                f"Item '{item.title}' ({item.guid}) marked as not worth summarizing, skipping..."
            )
            # Add to results but don't summarize
            item.is_summarized = False  # Explicitly mark as not summarized
            summarized_items.append(item)
            items_skipped += 1
            continue

        items_to_summarize += 1

        # Generate summaries for this item (standard and brief)
        item = summarizer.summarize_item(item, summary_type="standard")
        item = summarizer.summarize_item(item, summary_type="brief")

        # Store summaries in S3
        if item.summary:
            summary_key = f"processed/summaries/{item.guid}.md"
            s3_storage.store_content(summary_key, item.summary)

        if item.short_summary:
            short_summary_key = f"processed/short_summaries/{item.guid}.md"
            s3_storage.store_content(short_summary_key, item.short_summary)

        # Update the item in DynamoDB
        state_manager.update_item(item)
        logger.info(
            f"Updated item '{item.title}' ({item.guid}): marked as summarized, added summaries"
        )

        summarized_items.append(item)

    # Log final summary
    logger.info(
        f"Summarization complete: {items_to_summarize} items summarized, {items_skipped} items skipped"
    )

    return summarized_items


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

    # Get and format recent content
    curated_content, included_items = curator.curate_recent_content(
        most_recent=most_recent, n_days=n_days, summary_type=summary_type
    )

    if not curated_content:
        logger.warning("No content available for newsletter curation.")
        return ""

    # Create a timestamp for the filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    newsletter_id = f"newsletter_{timestamp}"

    # Save the curated content to S3 with timestamp
    s3_key = f"curated/{newsletter_id}.md"
    if s3_storage.store_content(s3_key, curated_content):
        logger.info(f"Newsletter saved to S3 at {s3_key}")
    else:
        logger.error("Failed to save newsletter to S3")

    # Also save at a fixed location as "latest.md"
    latest_key = "curated/latest.md"
    if s3_storage.store_content(latest_key, curated_content):
        logger.info(f"Newsletter saved to S3 at {latest_key} (latest version)")
    else:
        logger.error("Failed to save latest newsletter to S3")

    return curated_content


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
    # Load environment variables
    load_dotenv()

    # Parse command line arguments
    args = parse_arguments()

    # Initialize services
    state_manager, s3_storage = setup_services()

    # Initialize variables to track items through pipeline
    fetched_items: List[ContentItem] = []
    processed_items: List[ContentItem] = []
    summarized_items: List[ContentItem] = []

    # Run fetch stage if enabled
    if args.fetch:
        fetched_items = run_fetch_stage(
            state_manager, s3_storage, args.id, args.rss_url, args.fetch_max_items
        )

    # Run process stage if enabled
    if args.process:
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


if __name__ == "__main__":
    main()
