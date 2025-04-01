import argparse
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger

from src.content_curator.fetchers.rss_fetcher import RssFetcher
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
    parser.add_argument("--all", action="store_true", help="Run all pipeline stages")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing processed or summarized content",
    )

    # Parse the arguments
    args = parser.parse_args()

    # If no arguments provided or --all specified, run all stages
    if not (args.fetch or args.process or args.summarize) or args.all:
        args.fetch = True
        args.process = True
        args.summarize = True

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


def run_fetch_stage(state_manager: DynamoDBState) -> List[Dict]:
    """Run the fetch stage to get new content."""
    # Initialize fetcher
    rss_url_file: Path = Path(__file__).parent / "data" / "rss_urls.txt"
    fetcher: RssFetcher = RssFetcher(url_file_path=str(rss_url_file), max_items=5)

    logger.info("Fetching content...")
    fetched_items: List[Dict] = fetcher.run()

    if not fetched_items:
        logger.warning("No items were fetched.")
        return []

    logger.info(f"--- Fetched {len(fetched_items)} items ---")

    # Store metadata for all fetched items
    for item in fetched_items:
        guid: str = item.get("guid", str(uuid.uuid4()))
        if not item.get("guid"):
            item["guid"] = guid

        # Check if item already exists
        if state_manager.item_exists(guid):
            # Only update fetch-related fields
            updates = {
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "published_date": item.get("published_date", ""),
                "fetch_date": item.get("fetch_date", ""),
                "source_url": item.get("source_url", ""),
                "is_fetched": True,
                "last_updated": datetime.now().isoformat(),
            }
            state_manager.update_metadata(guid=guid, updates=updates)
            logger.info(f"Updated fetch metadata for item: {guid}")
            logger.debug(
                f"Fetch updates: {', '.join([f'{k}={v}' for k, v in updates.items() if k != 'last_updated'])}"
            )
        else:
            # Store metadata without HTML content
            metadata: Dict[str, str] = {
                "guid": guid,
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "published_date": item.get("published_date", ""),
                "fetch_date": item.get("fetch_date", ""),
                "source_url": item.get("source_url", ""),
                "is_fetched": True,
                "is_processed": False,
                "is_summarized": False,
                "is_distributed": False,
                "last_updated": datetime.now().isoformat(),
            }
            # Store metadata in DynamoDB
            state_manager.store_metadata(metadata)
            logger.info(
                f"Created new item metadata: {guid} - '{item.get('title', '')}'"
            )

        # Store HTML content in S3 instead of DynamoDB
        html_content = item.get("html_content", "")
        if html_content:
            html_key = f"html/{guid}.html"
            s3_storage.store_content(html_key, html_content)
            # Add the html content reference to the item for processing
            item["html_path"] = html_key
            logger.debug(f"Stored HTML content at: {html_key}")

    return fetched_items


def run_process_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    fetched_items: List[Dict],
    fetch_flag: bool,
    overwrite_flag: bool,
) -> List[Dict]:
    """Run the processing stage to convert HTML to markdown."""
    # If we didn't fetch items or fetching was disabled, get items from DynamoDB
    if not fetch_flag or not fetched_items:
        logger.info("Loading items for processing from database...")
        if overwrite_flag:
            # If overwrite is enabled, get all fetched items regardless of processing status
            logger.info("Overwrite flag enabled - getting all fetched items...")
            fetched_items = state_manager.get_items_by_status_flags(is_fetched=True)
            fetched_items2 = state_manager.get_items_by_status_flags(is_fetched=False)
            fetched_items.extend(fetched_items2)
        else:
            # Otherwise, get only items that need processing
            fetched_items = state_manager.get_items_needing_processing("processed")

        if not fetched_items:
            logger.warning("No items found for processing.")
            return []

        logger.info(f"--- Loaded {len(fetched_items)} items for processing ---")

        # Get HTML content from S3 for each item if needed
        for item in fetched_items:
            guid = item.get("guid", "")
            if "html_content" not in item:
                html_path = item.get("html_path", f"html/{guid}.html")
                html_content = s3_storage.get_content(html_path)
                if html_content:
                    item["html_content"] = html_content

    # Initialize processor
    processor: MarkdownProcessor = MarkdownProcessor()

    logger.info("Processing content...")
    processed_items: List[Dict] = processor.process_content(fetched_items)

    # Store processed content to AWS (S3 and DynamoDB)
    logger.info("Storing processed content to AWS...")
    for item in processed_items:
        guid: str = item.get("guid", str(uuid.uuid4()))
        if not item.get("guid"):
            item["guid"] = guid

        markdown_content: Optional[str] = item.get("markdown_content", "")
        if not markdown_content:
            logger.warning(f"No markdown content to store for item {guid}")
            continue

        # Store in S3
        s3_key: str = f"markdown/{guid}.md"
        if s3_storage.store_content(s3_key, markdown_content):
            # Update only the processing-related metadata
            updates = {
                "s3_path": s3_key,
                "is_processed": True,
                "last_updated": datetime.now().isoformat(),
            }

            # Add paywall and summarization flags if available
            if "is_paywall" in item:
                updates["is_paywall"] = item["is_paywall"]
            if "to_be_summarized" in item:
                updates["to_be_summarized"] = item["to_be_summarized"]

            # Update metadata in DynamoDB
            state_manager.update_metadata(guid=guid, updates=updates)

            # Create a concise overview of the update
            update_info = []
            if updates.get("is_processed") == True:
                update_info.append("marked as processed")
            if updates.get("s3_path"):
                update_info.append("added S3 path")
            if "is_paywall" in updates:
                update_info.append(f"paywall: {updates['is_paywall']}")
            if "to_be_summarized" in updates:
                update_info.append(f"to summarize: {updates['to_be_summarized']}")

            # Log a clear, concise update message
            logger.info(f"Updated item {guid}: {', '.join(update_info)}")

    return processed_items


def run_summarize_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    processed_items: List[Dict],
    process_flag: bool,
    overwrite_flag: bool,
) -> List[Dict]:
    """Run the summarization stage to generate summaries."""
    # If we didn't process items or processing was disabled, get processed items from DynamoDB
    if not process_flag or not processed_items:
        logger.info("Loading items that have been processed for summarization...")
        if overwrite_flag:
            # If overwrite is enabled, get all processed items regardless of summarization status
            logger.info("Overwrite flag enabled - getting all processed items...")
            db_processed_items = state_manager.get_items_by_status_flags(
                is_processed=True,
                limit=100,
            )
        else:
            # Otherwise, get only items that need summarization
            db_processed_items = state_manager.get_items_by_status_flags(
                is_processed=True,
                is_summarized=False,
                limit=100,
            )

        if not db_processed_items:
            logger.warning("No items found for summarization.")
            return []

        logger.info(f"--- Loaded {len(db_processed_items)} items for summarization ---")

        # We need to fetch the actual content for these items
        processed_items = []
        for item in db_processed_items:
            s3_path = item.get("s3_path")
            if s3_path:
                # Get markdown content from S3
                markdown_content = s3_storage.get_content(s3_path)
                if markdown_content:
                    item["markdown_content"] = markdown_content
                    processed_items.append(item)
                else:
                    logger.warning(f"Could not retrieve content for {item.get('guid')}")

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
        guid = item.get("guid")

        # Check if item is already summarized and we're not in overwrite mode
        if not overwrite_flag and item.get("is_summarized"):
            logger.info(f"Item {guid} already summarized, skipping...")
            summarized_items.append(item)
            continue

        # Check if this item should be summarized
        # If item was just processed, it will have the to_be_summarized flag
        # Otherwise, check paywall and content length directly
        to_be_summarized = item.get("to_be_summarized")

        # If to_be_summarized flag is not present (might be older data)
        # we'll use a processor instance to check
        if to_be_summarized is None and item.get("markdown_content"):
            processor = MarkdownProcessor()
            to_be_summarized = processor.is_worth_summarizing(
                item.get("markdown_content", "")
            )

            # If the item is determined not to be worth summarizing, update its metadata
            if not to_be_summarized:
                is_paywall = processor.is_paywall_or_teaser(
                    item.get("markdown_content", "")
                )
                state_manager.update_metadata(
                    guid=guid,
                    updates={
                        "is_paywall": is_paywall,
                        "to_be_summarized": False,
                        "last_updated": datetime.now().isoformat(),
                    },
                )

        # Skip items not worth summarizing
        if not to_be_summarized:
            logger.warning(
                f"Item {guid} marked as not worth summarizing, skipping...\n{item.get('markdown_content', '')}"
            )
            # Add to results but don't summarize
            item["is_summarized"] = False  # Explicitly mark as not summarized
            summarized_items.append(item)
            items_skipped += 1
            continue

        items_to_summarize += 1

        # Generate summary for this item
        summary_result = summarizer.summarize_text(
            item.get("markdown_content", ""), summary_type="standard"
        )
        if summary_result:
            item["summary"] = summary_result

            # Store summary in S3
            summary_key = f"processed/summaries/{guid}.md"
            if s3_storage.store_content(summary_key, summary_result):
                # Update only the summarization-related metadata
                updates = {
                    "is_summarized": True,
                    "summary_path": summary_key,
                    "to_be_summarized": True,  # Make sure we record this decision
                    "last_updated": datetime.now().isoformat(),
                }

                # Generate brief summary if needed
                short_summary_result = summarizer.summarize_text(
                    item.get("markdown_content", ""), summary_type="brief"
                )
                if short_summary_result:
                    item["short_summary"] = short_summary_result
                    short_summary_key = f"processed/short_summaries/{guid}.md"

                    if s3_storage.store_content(
                        short_summary_key, short_summary_result
                    ):
                        updates["short_summary_path"] = short_summary_key

                # Update metadata in DynamoDB
                state_manager.update_metadata(guid=guid, updates=updates)

                # Create a concise overview of the update
                update_overview = [
                    "marked as summarized",
                    f"standard summary: {summary_key}",
                ]
                if "short_summary_path" in updates:
                    update_overview.append(f"brief summary: {short_summary_key}")

                # Log a clear, concise update message
                logger.info(f"Updated item {guid}: {', '.join(update_overview)}")
        else:
            logger.warning(f"No summary generated for item {guid}")

        summarized_items.append(item)

    logger.info(
        f"Summarization complete: {items_to_summarize} items processed, {items_skipped} items skipped"
    )
    return summarized_items


def save_last_item(processed_items: List[Dict], summarize_flag: bool):
    """Save the last processed item's content to a local file. For testing and debugging."""
    if not processed_items:
        return

    try:
        last_item = processed_items[-1]
        markdown_content = last_item.get("markdown_content", "")
        output_path = "/tmp/last_processed_item.md"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        logger.info(f"Saved last item's markdown content to {output_path}")

        if summarize_flag and last_item.get("summary"):
            summary_path = "/tmp/last_item_summary.md"
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(last_item.get("summary", ""))
            logger.info(f"Saved last item's summary to {summary_path}")
    except Exception as e:
        logger.error(f"Error saving markdown content: {e}")


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Load environment variables from .env file
    load_dotenv()

    logger.info("Starting content curation process...")
    logger.info(
        f"Pipeline stages: Fetch={args.fetch}, Process={args.process}, Summarize={args.summarize}"
    )

    # Setup services
    state_manager, s3_storage = setup_services()

    # Initialize tracking for pipeline stages
    fetched_items = []
    processed_items = []
    summarized_items = []

    # Step 1: Fetch raw content (if enabled)
    if args.fetch:
        fetched_items = run_fetch_stage(state_manager)
        if not fetched_items and not (args.process or args.summarize):
            logger.warning(
                "No items were fetched and no further stages enabled. Exiting."
            )
            sys.exit(0)

    # Step 2: Process content (if enabled)
    if args.process:
        processed_items = run_process_stage(
            state_manager, s3_storage, fetched_items, args.fetch, args.overwrite
        )
        if not processed_items and not args.summarize:
            logger.warning(
                "No items were processed and no further stages enabled. Exiting."
            )
            sys.exit(0)

    # Step 3: Summarize the content (if enabled)
    if args.summarize:
        summarized_items = run_summarize_stage(
            state_manager, s3_storage, processed_items, args.process, args.overwrite
        )
        if not summarized_items:
            logger.warning("No items were summarized. Exiting.")
            sys.exit(0)

    # Display summary of what was done
    logger.info("--- Pipeline execution completed ---")
