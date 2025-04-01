import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))

from src.content_curator.models import ContentItem
from src.content_curator.storage.dynamodb_state import DynamoDBState
from src.content_curator.utils import generate_url_hash


def update_guids(dry_run: bool = True, batch_size: int = 25):
    """
    Update all GUIDs in the DynamoDB table by generating new hashes from the 'link' field.

    Args:
        dry_run: If True, only show what would be updated without making changes
        batch_size: Number of items to process at once to avoid overwhelming DynamoDB
    """
    # Load environment variables
    load_dotenv()

    # Get AWS configuration
    dynamodb_table_name: str = os.getenv(
        "AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata"
    )
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")

    # Initialize DynamoDB state manager
    state_manager = DynamoDBState(
        dynamodb_table_name=dynamodb_table_name, aws_region=aws_region
    )

    # Get all items from the table as ContentItem objects
    all_items = state_manager.get_all_items(as_content_items=True)

    if not all_items:
        logger.warning("No items found in the table.")
        return

    logger.info(f"Found {len(all_items)} items to process")

    # Keep track of stats
    updates_needed = 0
    updates_made = 0
    errors = 0
    skipped = 0

    # Keep track of new GUIDs to check for collisions
    new_guids = {}  # Map of new_guid -> old_guid for collision detection
    items_to_update = []  # List of (item, new_guid) tuples that need updating

    # First pass: check what needs to be updated and verify no collisions
    for item in all_items:
        old_guid = item.guid
        link = item.link

        if not link:
            logger.warning(f"Item {old_guid} has no link field, skipping")
            skipped += 1
            continue

        new_guid = generate_url_hash(link)

        if new_guid == old_guid:
            logger.debug(f"Item {old_guid} already has correct GUID format")
            continue

        if new_guid in new_guids:
            logger.error(
                f"COLLISION DETECTED! Links:\n"
                f"1. {link}\n"
                f"2. Item with old_guid: {new_guids[new_guid]}\n"
                f"Both generate GUID: {new_guid}"
            )
            errors += 1
            continue

        new_guids[new_guid] = old_guid
        items_to_update.append((item, new_guid))
        updates_needed += 1

        logger.info(f"Will update: {old_guid} -> {new_guid} (from {link})")

    if errors > 0:
        logger.error(f"Found {errors} errors. Please fix these before proceeding.")
        return

    if updates_needed == 0:
        logger.info("No updates needed - all GUIDs are already in the correct format.")
        return

    if dry_run:
        logger.info(f"""
DRY RUN Summary:
- Total items: {len(all_items)}
- Updates needed: {updates_needed}
- Items skipped: {skipped}
- Errors: {errors}
        """)
        return

    # Second pass: perform the updates in batches
    logger.info(f"Proceeding to update {updates_needed} items...")

    for i in range(0, len(items_to_update), batch_size):
        batch = items_to_update[i : i + batch_size]
        logger.info(
            f"Processing batch {i // batch_size + 1} of {(len(items_to_update) + batch_size - 1) // batch_size}"
        )

        for item, new_guid in batch:
            old_guid = item.guid
            try:
                # Create new ContentItem with updated GUID
                # Convert to dict and back to make a deep copy
                item_dict = item.to_dict()

                # Create new ContentItem with new GUID
                new_item = ContentItem(
                    guid=new_guid,
                    link=item.link,
                    # Copy all other attributes from original item
                    title=item.title,
                    published_date=item.published_date,
                    fetch_date=item.fetch_date,
                    source_url=item.source_url,
                    is_fetched=item.is_fetched,
                    is_processed=item.is_processed,
                    is_summarized=item.is_summarized,
                    is_distributed=item.is_distributed,
                    is_paywall=item.is_paywall,
                    to_be_summarized=item.to_be_summarized,
                    html_path=item.html_path,
                    md_path=item.md_path,
                    summary_path=item.summary_path,
                    short_summary_path=item.short_summary_path,
                    newsletters=item.newsletters,
                )

                # Update timestamp
                new_item.last_updated = datetime.now().isoformat()

                # Store new item
                if state_manager.store_item(new_item):
                    # Only delete old item if new one was stored successfully
                    if state_manager.delete_item(old_guid):
                        updates_made += 1
                        logger.info(f"Updated item {old_guid} -> {new_guid}")

            except Exception as e:
                logger.error(f"Error updating item {old_guid}: {e}")
                errors += 1

    logger.info(f"""
Update complete:
- Items processed: {len(all_items)}
- Updates needed: {updates_needed}
- Updates made: {updates_made}
- Errors: {errors}
    """)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Update GUIDs in DynamoDB table")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the updates. Without this flag, runs in dry-run mode.",
    )

    args = parser.parse_args()

    update_guids(dry_run=not args.execute)
