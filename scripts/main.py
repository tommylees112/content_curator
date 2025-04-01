import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import yaml
from dotenv import load_dotenv
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from src.content_curator.config import config
from src.content_curator.curator.newsletter_curator import NewsletterCurator
from src.content_curator.distributors.email_distributor import EmailDistributor
from src.content_curator.fetchers.rss_fetcher import RSSFetcher
from src.content_curator.models import ContentItem
from src.content_curator.processors.markdown_processor import MarkdownProcessor
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.storage.s3_storage import S3Storage
from src.content_curator.summarizers.summarizer import Summarizer
from src.content_curator.utils import check_resources

# Configure logging
# logger.add(
#     config.pipeline_log_file_path,
#     rotation=config.log_rotation,
#     retention=config.log_retention,
#     level=config.log_level,
#     format=config.log_format,
# )


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
    parser.add_argument(
        "--distribute",
        action="store_true",
        help="Run the distribution stage to send newsletters via email",
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
        "--rss_url_file",
        type=str,
        help="Path to the text file containing RSS feed URLs (one per line).",
        default=config.rss_url_file,
    )

    parser.add_argument(
        "--fetch_max_items",
        type=int,
        default=config.rss_default_max_items,
        help=f"Maximum number of most recent items to fetch per feed (default: {config.rss_default_max_items}). Use 0 for no limit.",
    )
    parser.add_argument(
        "--full-summary",
        action="store_true",
        help="Generate both brief and full summaries (default: brief only)",
    )
    parser.add_argument(
        "--most_recent",
        type=int,
        default=config.default_most_recent,
        help=f"Number of most recent items to include in newsletters (default: {config.default_most_recent})",
    )
    parser.add_argument(
        "--summary_types",
        type=str,
        nargs="+",
        default=config.default_summary_types,
        help=f"Types of summaries to generate (default: {config.default_summary_types})",
    )
    parser.add_argument(
        "--recipient_email",
        type=str,
        help="Email address to send the newsletter to. Defaults to the configured default_recipient.",
    )
    parser.add_argument(
        "--s3_key",
        type=str,
        default="curated/latest.md",
        help="S3 key of the newsletter to distribute. Defaults to 'curated/latest.md'.",
    )

    # Parse the arguments
    args = parser.parse_args()

    # If RSS URL is provided, enable appropriate stages
    if args.rss_url:
        # When an RSS URL is provided, enable all stages by default unless specific stages are requested
        if not (
            args.fetch
            or args.process
            or args.summarize
            or args.curate
            or args.distribute
        ):
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
    elif (
        not (
            args.fetch
            or args.process
            or args.summarize
            or args.curate
            or args.distribute
        )
        or args.all
    ):
        args.fetch = True
        args.process = True
        args.summarize = True
        args.curate = True
        args.distribute = True

    return args


def setup_services() -> Tuple[DynamoDBState, S3Storage]:
    """Initialize and check AWS services."""
    # Initialize services with config values
    state_manager = DynamoDBState(
        dynamodb_table_name=config.dynamodb_table_name,
        aws_region=config.aws_region,
    )
    s3_storage = S3Storage(
        s3_bucket_name=config.s3_bucket_name,
        aws_region=config.aws_region,
    )

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
    rss_url_file: Optional[Path] = Path(__file__).parent / "data" / "rss_urls.txt",
    fetch_max_items: Optional[int] = None,
    overwrite_flag: bool = False,
) -> List[ContentItem]:
    """Run the fetch stage to get new content."""
    # Initialize fetcher with either a file of URLs or a specific RSS URL
    if rss_url:
        # Create RSSFetcher with the specific RSS URL
        logger.info(f"Fetching from RSS URL: {rss_url}")
        fetcher = RSSFetcher(
            max_items=fetch_max_items or config.rss_default_max_items,
            specific_url=rss_url,
            s3_storage=s3_storage,
            state_manager=state_manager,
        )
    else:
        # Use the file of RSS URLs
        fetcher = RSSFetcher(
            url_file_path=str(rss_url_file),
            max_items=fetch_max_items or config.rss_default_max_items,
            s3_storage=s3_storage,
            state_manager=state_manager,
        )

    logger.info("Fetching content...")
    return fetcher.fetch_and_update_state(specific_id, overwrite_flag)


def run_process_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    fetched_items: List[ContentItem],
    fetch_flag: bool,
    overwrite_flag: bool,
    specific_id: Optional[str] = None,
    fetch_max_items: Optional[int] = None,
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
        logger.debug(f"Using {len(items_to_process)} items passed from fetch stage")
    else:
        # Otherwise, get items from the database using the helper method
        # Use fetch_max_items as limit if provided, otherwise use default
        limit = fetch_max_items if fetch_max_items is not None else 100
        logger.debug(f"Querying for items to process with limit: {limit}")
        items_to_process = state_manager.get_items_for_stage(
            stage="process",
            specific_id=specific_id,
            overwrite_flag=overwrite_flag,
            limit=limit,
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
    full_summary: bool = False,
    fetch_max_items: Optional[int] = None,
    summary_types: List[str] = None,
) -> List[ContentItem]:
    """
    Run the summarization stage to generate summaries.

    Args:
        state_manager: DynamoDB state manager
        s3_storage: S3 storage manager
        processed_items: Items from process stage
        process_flag: Whether process stage was run
        overwrite_flag: Whether to overwrite existing summaries
        specific_id: Optional specific item ID to process
        full_summary: If True, generate both brief and full summaries
        fetch_max_items: Maximum number of items to process
        summary_types: List of summary types to generate
    """
    # Initialize summarizer
    logger.info("Summarizing content...")
    summarizer = Summarizer(
        model_name=config.summarizer_model_name,
        s3_storage=s3_storage,
        state_manager=state_manager,
    )

    # If we got items from process stage, use those
    if process_flag and processed_items:
        items_to_summarize = processed_items
        logger.debug(f"Using {len(items_to_summarize)} items passed from process stage")
    else:
        # Otherwise, get items from the database using the helper method
        # Use fetch_max_items as limit if provided, otherwise use default
        limit = fetch_max_items if fetch_max_items is not None else 100
        logger.debug(f"Querying for items to summarize with limit: {limit}")
        items_to_summarize = state_manager.get_items_for_stage(
            stage="summarize",
            specific_id=specific_id,
            overwrite_flag=overwrite_flag,
            limit=limit,
        )
        if not items_to_summarize:
            logger.warning("No items found for summarization.")
            return []

    logger.info(f"--- Loaded {len(items_to_summarize)} items for summarization ---")

    # Determine summary types based on full_summary flag
    types_to_generate = summary_types or config.default_summary_types
    if full_summary:
        types_to_generate = ["brief", "standard"]
    logger.info(f"Generating summary types: {types_to_generate}")

    # Summarize items and update state
    return summarizer.summarize_and_update_state(
        items_to_summarize, overwrite_flag, summary_types=types_to_generate
    )


def run_curate_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    most_recent: Optional[int] = None,
    n_days: Optional[int] = None,
    summary_type: str = "brief",
) -> str:
    """Run the curation stage to create newsletters and save them to S3."""
    logger.info("Creating newsletter from recent content...")

    # Initialize the newsletter curator
    curator = NewsletterCurator(state_manager=state_manager, s3_storage=s3_storage)

    # Generate newsletter and save to S3
    return curator.curate_and_update_state(
        most_recent=most_recent or config.default_most_recent,
        n_days=n_days,
        summary_type=summary_type,
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


def run_distribute_stage(
    state_manager: DynamoDBState,
    s3_storage: S3Storage,
    s3_key: str = "curated/latest.md",
    recipient_email: Optional[str] = None,
) -> bool:
    """Run the distribution stage to send newsletters via email.

    Args:
        state_manager: DynamoDB state manager
        s3_storage: S3 storage manager
        s3_key: The S3 key of the newsletter to distribute
        recipient_email: Email address to send to (uses configured default if None)

    Returns:
        True if email was successfully sent, False otherwise
    """
    logger.info("Distributing newsletter via email...")

    # Initialize the email distributor
    distributor = EmailDistributor(s3_storage=s3_storage)

    # Send the email
    return distributor.distribute(
        s3_key=s3_key,
        recipient_email=recipient_email,
    )


def main():
    """Main entry point for the content curation pipeline."""
    logger.info(f"\n{'-' * 50}\nmain.py execution started\n{'-' * 50}\n")

    # Log the configuration in YAML format
    logger.info(
        "Configuration:\n{}",
        yaml.dump(config.config, default_flow_style=False, sort_keys=False),
    )

    # Load environment variables
    load_dotenv()

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
            state_manager,
            s3_storage,
            args.id,
            args.rss_url,
            args.rss_url_file,
            args.fetch_max_items,
            args.overwrite,
        )
        logger.info(f"Fetch stage completed with {len(fetched_items)} items")
        for item in fetched_items:
            logger.debug(f"Fetched item: {item.guid} - {item.title}")

    # Run process stage if enabled
    if args.process:
        logger.info("\n\nRunning process stage...\n\n".upper())
        logger.debug(
            f"Process stage starting with {len(fetched_items)} items from fetch stage"
        )
        processed_items = run_process_stage(
            state_manager,
            s3_storage,
            fetched_items,
            args.fetch,
            args.overwrite,
            args.id,
            args.fetch_max_items,
        )
        logger.info(f"Process stage completed with {len(processed_items)} items")
        for item in processed_items:
            logger.debug(f"Processed item: {item.guid} - {item.title}")

        # Save last processed item if requested
        if args.save_locally:
            save_last_item(processed_items, args.summarize)

    # Run summarize stage if enabled
    if args.summarize:
        logger.info("\n\nRunning summarize stage...\n\n".upper())
        logger.debug(
            f"Summarize stage starting with {len(processed_items)} items from process stage"
        )
        summarized_items = run_summarize_stage(
            state_manager,
            s3_storage,
            processed_items,
            args.process,
            args.overwrite,
            args.id,
            full_summary=args.full_summary,
            fetch_max_items=args.fetch_max_items,
            summary_types=args.summary_types,
        )
        logger.info(f"Summarize stage completed with {len(summarized_items)} items")
        for item in summarized_items:
            logger.debug(f"Summarized item: {item.guid} - {item.title}")

        # Save last summarized item if requested (and not already saved in process stage)
        if args.save_locally and not args.process:
            save_last_item(summarized_items, args.summarize)

    # Run curate stage if enabled
    if args.curate:
        logger.info("\n\nRunning curate stage...\n\n".upper())
        # Get summary types from config
        summary_types_for_curation = config.curator_content_summary_types
        logger.info(
            f"Curating newsletters for summary types: {summary_types_for_curation}"
        )

        for summary_type in summary_types_for_curation:
            logger.info(f"Running curation for summary type: {summary_type}")
            curated_content = run_curate_stage(
                state_manager,
                s3_storage,
                most_recent=args.most_recent,
                summary_type=summary_type,
            )
            # Optionally save to local file for debugging
            if curated_content and args.save_locally:
                try:
                    output_path = f"/tmp/latest_newsletter_{summary_type}.md"
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(curated_content)
                    logger.info(
                        f"Saved latest {summary_type} newsletter to {output_path}"
                    )
                except Exception as e:
                    logger.error(f"Error saving {summary_type} newsletter content: {e}")

    # Run distribute stage if enabled
    if args.distribute:
        logger.info("\n\nRunning distribute stage...\n\n".upper())
        success = run_distribute_stage(
            state_manager,
            s3_storage,
            s3_key=args.s3_key,
            recipient_email=args.recipient_email,
        )
        if success:
            logger.info("Distribution stage completed successfully")
        else:
            logger.error("Distribution stage failed")

    logger.info(f"Pipeline completed. Log file: {config.log_file}")


if __name__ == "__main__":
    main()
