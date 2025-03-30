import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from src.content_curator.aws_storage import AwsStorage
from src.content_curator.fetchers.rss_fetcher import RssFetcher
from src.content_curator.processors.content_processor import ContentProcessor

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    logger.info("Starting content curation process...")

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

    # Check if AWS resources exist (they should be created via Terraform)
    if not aws_storage.check_resources_exist():
        logger.error(
            "Required AWS resources do not exist. Please ensure your S3 bucket and DynamoDB table are created."
        )
        sys.exit(1)

    # Initialize fetcher
    rss_url_file = (
        Path(__file__).parent / "data" / "rss_urls.txt"
    )  # Make sure this file exists
    fetcher = RssFetcher(url_file_path=rss_url_file)

    # Initialize processor
    processor = ContentProcessor()

    # Step 1: Fetch raw content
    logger.info("Fetching content...")
    fetched_items = fetcher.run()

    if not fetched_items:
        logger.warning("No items were fetched.")
        exit(0)

    logger.info(f"--- Fetched {len(fetched_items)} items ---")

    # Step 2: Process content (convert HTML to markdown, format, etc.)
    logger.info("Processing content...")
    processed_items = processor.process_content(fetched_items)

    # Step 3: Store processed content to AWS (S3 and DynamoDB)
    logger.info("Storing content to AWS...")
    stored_items = aws_storage.store_content(processed_items)

    # Display sample of processed content
    logger.info(f"--- Processed and stored {len(stored_items)} items ---")

    # Example: Print titles and snippet of markdown for the first few items
    for i, item in enumerate(stored_items[:5]):  # Print first 5
        logger.info(f"Item {i + 1}:")
        logger.info(f"  Title: {item.get('title')}")
        logger.info(f"  Link: {item.get('link')}")
        logger.info(f"  GUID: {item.get('guid')}")
        logger.info(f"  Source Feed: {item.get('source_url')}")
        logger.info(f"  S3 Path: {item.get('s3_path')}")
        markdown_snippet = item.get("markdown_content", "")[:200]  # Get first 200 chars
        logger.info(f"  Markdown Snippet: {markdown_snippet}...")

    # Save the last snippet to a markdown file (local preview)
    try:
        if stored_items:
            last_item = stored_items[:5][-1]
            markdown_content = last_item.get("markdown_content", "")
            output_path = "/Users/tommylees/Downloads/test.md"

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            logger.info(f"Saved last item's markdown content to {output_path}")
    except Exception as e:
        logger.error(f"Error saving markdown content: {e}")

    # Example of how to use other AWS storage functions
    # Commented out since they would be used in later processing steps

    # # Update status for items after processing
    # for item in stored_items:
    #     guid = item.get("guid")
    #     aws_storage.update_processing_status(guid, "summarized")
    #
    # # Store a processed summary
    # summary_content = "This is a summary of multiple articles..."
    # aws_storage.store_processed_summary(stored_items[0].get("guid"), summary_content)
    #
    # # Create a daily update
    # from datetime import datetime
    # update_id = datetime.now().strftime("%Y-%m-%d")
    # daily_update_content = "# Daily Update\n\nToday's news summary..."
    # aws_storage.store_daily_update(update_id, daily_update_content)
