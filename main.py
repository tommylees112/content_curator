import argparse
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

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

    # Parse the arguments
    args = parser.parse_args()

    # If no arguments provided or --all specified, run all stages
    if not (args.fetch or args.process or args.summarize) or args.all:
        args.fetch = True
        args.process = True
        args.summarize = True

    return args


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Load environment variables from .env file
    load_dotenv()

    logger.info("Starting content curation process...")
    logger.info(
        f"Pipeline stages: Fetch={args.fetch}, Process={args.process}, Summarize={args.summarize}"
    )

    # Get AWS configuration from environment variables
    s3_bucket_name: str = os.getenv("AWS_S3_BUCKET_NAME", "content-curator-bucket")
    dynamodb_table_name: str = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")

    # Initialize services
    state_manager: DynamoDBState = DynamoDBState(
        dynamodb_table_name=dynamodb_table_name, aws_region=aws_region
    )
    s3_storage: S3Storage = S3Storage(
        s3_bucket_name=s3_bucket_name, aws_region=aws_region
    )

    # Check if resources exist
    s3_exists: bool = check_resources(s3_storage)
    dynamo_exists: bool = check_resources(state_manager)

    if not (s3_exists and dynamo_exists):
        logger.error(
            "Required AWS resources do not exist. Please ensure your S3 bucket and DynamoDB table are created."
        )
        sys.exit(1)

    processed_items = []

    # Step 1: Fetch raw content (if enabled)
    if args.fetch:
        # Initialize fetcher
        rss_url_file: Path = (
            Path(__file__).parent / "data" / "rss_urls.txt"
        )  # Make sure this file exists
        fetcher: RssFetcher = RssFetcher(url_file_path=str(rss_url_file))

        logger.info("Fetching content...")
        fetched_items: List[Dict] = fetcher.run()

        if not fetched_items:
            logger.warning("No items were fetched.")
            if args.process or args.summarize:
                logger.info("Continuing with existing items from database...")
            else:
                sys.exit(0)
        else:
            logger.info(f"--- Fetched {len(fetched_items)} items ---")

            # If processing is not enabled, store the raw fetched items
            if not args.process:
                for item in fetched_items:
                    guid: str = item.get("guid", str(uuid.uuid4()))
                    if not item.get("guid"):
                        item["guid"] = guid

                    # Store metadata without markdown content
                    metadata: Dict[str, str] = {
                        "guid": guid,
                        "title": item.get("title", ""),
                        "link": item.get("link", ""),
                        "published_date": item.get("published_date", ""),
                        "fetch_date": item.get("fetch_date", ""),
                        "source_url": item.get("source_url", ""),
                        "html_content": item.get("html_content", ""),
                        "is_fetched": True,  # Set fetched status to True
                        "is_processed": True,  # Set processed status to True if this is from the process stage
                        "is_summarized": False,  # Initialize as False
                        "is_distributed": False,  # Initialize as False
                        "last_updated": datetime.now().isoformat(),
                    }

                    # Store metadata in DynamoDB
                    state_manager.store_metadata(metadata)

    # Step 2: Process content (if enabled)
    if args.process:
        # If we didn't fetch items or fetching was disabled, get items from DynamoDB
        if not args.fetch or not fetched_items:
            logger.info("Loading items with 'processed' status from database...")
            fetched_items = state_manager.get_items_needing_processing("processed")

            if not fetched_items:
                logger.warning("No items found for processing.")
                if args.summarize:
                    logger.info(
                        "Continuing with existing processed items from database..."
                    )
                else:
                    sys.exit(0)
            else:
                logger.info(f"--- Loaded {len(fetched_items)} items for processing ---")

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
                # Create metadata for DynamoDB or update existing
                metadata: Dict[str, str] = {
                    "guid": guid,
                    "title": item.get("title", ""),
                    "link": item.get("link", ""),
                    "published_date": item.get("published_date", ""),
                    "fetch_date": item.get("fetch_date", ""),
                    "source_url": item.get("source_url", ""),
                    "s3_path": s3_key,
                    "is_fetched": True,  # Set fetched status to True
                    "is_processed": True,  # Set processed status to True if this is from the process stage
                    "is_summarized": False,  # Initialize as False
                    "is_distributed": False,  # Initialize as False
                    "last_updated": datetime.now().isoformat(),
                }

                # Store/update metadata in DynamoDB
                if state_manager.item_exists(guid):
                    # Update existing item
                    state_manager.update_metadata(
                        guid=guid,
                        updates={
                            "s3_path": s3_key,
                            "is_processed": True,  # Set processed status to True
                            "last_updated": datetime.now().isoformat(),
                        },
                    )
                else:
                    # Store new metadata
                    state_manager.store_metadata(metadata)

    # Step 3: Summarize the content (if enabled)
    if args.summarize:
        # If we didn't process items or processing was disabled, get processed items from DynamoDB
        if not args.process or not processed_items:
            logger.info("Loading items that have been processed but not summarized...")
            db_processed_items = state_manager.get_items_by_stage_status(
                stage="processed",  # Items that have been processed
                status=True,  # Status is True (processed=True)
                limit=100,
            )

            if not db_processed_items:
                logger.warning("No items found for summarization.")
                sys.exit(0)
            else:
                logger.info(
                    f"--- Loaded {len(db_processed_items)} items for summarization ---"
                )

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
                            logger.warning(
                                f"Could not retrieve content for {item.get('guid')}"
                            )

        if not processed_items:
            logger.warning("No items with content available for summarization.")
            sys.exit(0)

        # Initialize summarizer
        logger.info("Summarizing content...")
        summarizer = Summarizer()  # Initialize with default model
        summarized_items = summarizer.batch_summarize(processed_items)

        # Store summaries in S3 and update metadata
        for item in summarized_items:
            guid = item.get("guid")
            summary = item.get("summary")
            short_summary = item.get(
                "short_summary", ""
            )  # Get short summary if available

            if not summary:
                logger.warning(f"No summary generated for item {guid}")
                continue

            # Store summary in S3
            summary_key = f"processed/summaries/{guid}.md"
            short_summary_key = f"processed/short_summaries/{guid}.md"

            # Flag to track if we need to update metadata
            update_needed = False
            updates = {
                "is_summarized": True,  # Set summarized status to True
                "last_updated": datetime.now().isoformat(),
            }

            # Store main summary
            if s3_storage.store_content(summary_key, summary):
                updates["summary_path"] = summary_key
                update_needed = True

            # Store short summary if available
            if short_summary and s3_storage.store_content(
                short_summary_key, short_summary
            ):
                updates["short_summary_path"] = short_summary_key
                update_needed = True

            # Update metadata in DynamoDB
            if update_needed:
                state_manager.update_metadata(guid=guid, updates=updates)

    # Display summary of what was done
    logger.info("--- Pipeline execution completed ---")

    # Save the last processed item's content to a local file if available
    if processed_items:
        try:
            last_item = processed_items[-1]
            markdown_content = last_item.get("markdown_content", "")
            output_path = "/tmp/last_processed_item.md"  # More generic path than the user-specific one

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            logger.info(f"Saved last item's markdown content to {output_path}")

            if args.summarize and last_item.get("summary"):
                summary_path = "/tmp/last_item_summary.md"
                with open(summary_path, "w", encoding="utf-8") as f:
                    f.write(last_item.get("summary", ""))
                logger.info(f"Saved last item's summary to {summary_path}")
        except Exception as e:
            logger.error(f"Error saving markdown content: {e}")
