import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import boto3
from botocore.exceptions import ClientError
from loguru import logger


class AwsStorage:
    """
    Handles AWS storage operations for content curation, using S3 for storing
    markdown files and DynamoDB for storing metadata.
    """

    def __init__(
        self,
        s3_bucket_name: str,
        dynamodb_table_name: str,
        aws_region: str = "us-east-1",
    ):
        """
        Initialize AWS storage with bucket and table names.

        Args:
            s3_bucket_name: Name of the S3 bucket
            dynamodb_table_name: Name of the DynamoDB table
            aws_region: AWS region to use
        """
        self.s3_bucket_name = s3_bucket_name
        self.dynamodb_table_name = dynamodb_table_name
        self.aws_region = aws_region

        # Initialize S3 client
        self.s3 = boto3.client("s3", region_name=aws_region)

        # Initialize DynamoDB resource
        self.dynamodb = boto3.resource("dynamodb", region_name=aws_region)
        self.table = self.dynamodb.Table(dynamodb_table_name)

        self.logger = logger

    def check_resources_exist(self) -> bool:
        """
        Check if necessary AWS resources (S3 bucket and DynamoDB table) exist.
        Returns True if all resources exist, False otherwise.
        """
        resources_exist = True

        # Check if S3 bucket exists
        try:
            self.s3.head_bucket(Bucket=self.s3_bucket_name)
            self.logger.info(f"S3 bucket {self.s3_bucket_name} exists")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404" or error_code == "403":
                self.logger.error(
                    f"S3 bucket {self.s3_bucket_name} does not exist or is not accessible"
                )
                resources_exist = False
            else:
                self.logger.error(f"Error checking S3 bucket: {e}")
                resources_exist = False

        # Check if DynamoDB table exists
        try:
            self.table.table_status
            self.logger.info(f"DynamoDB table {self.dynamodb_table_name} exists")
        except ClientError as e:
            self.logger.error(
                f"DynamoDB table {self.dynamodb_table_name} does not exist or is not accessible: {e}"
            )
            resources_exist = False

        return resources_exist

    def store_single_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Store a single content item in S3 and its metadata in DynamoDB.

        Args:
            item: A processed content item

        Returns:
            The item with storage path added if successful, None otherwise
        """
        # Generate a unique filename if GUID isn't available
        guid = item.get("guid", str(uuid.uuid4()))

        # Store markdown in S3
        markdown_content = item.get("markdown_content", "")
        if not markdown_content:
            self.logger.warning(f"No markdown content to store for item {guid}")
            return None

        try:
            # Create path for markdown file: markdown/{guid}.md
            s3_key = f"markdown/{guid}.md"

            # Upload to S3
            self.s3.put_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key,
                Body=markdown_content,
                ContentType="text/markdown",
            )

            # Add S3 path to the item
            item["s3_path"] = s3_key

            # Create metadata for DynamoDB
            metadata = {
                "guid": guid,
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "published_date": item.get("published_date", ""),
                "fetch_date": item.get("fetch_date", ""),
                "source_url": item.get("source_url", ""),
                "s3_path": s3_key,
                "processing_status": "fetched",  # Initial status
                "last_updated": datetime.now().isoformat(),
            }

            # Store metadata in DynamoDB
            self.table.put_item(Item=metadata)

            self.logger.info(f"Stored content for item with GUID: {guid}")
            return item

        except Exception as e:
            self.logger.error(f"Error storing content for item {guid}: {e}")
            return None

    def store_content(
        self, processed_items: List[Dict[str, Any]], skip_existing: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Store processed content items in S3 and their metadata in DynamoDB.
        Each item is processed individually so failures don't affect other items.

        Args:
            processed_items: List of processed content items
            skip_existing: Whether to skip items that already exist in the database

        Returns:
            List of successfully processed items with storage paths added
        """
        stored_items = []
        failed_count = 0
        skipped_count = 0

        for item in processed_items:
            result = self.check_and_store_item(item, skip_if_exists=skip_existing)
            if result:
                stored_items.append(result)
            elif self.item_exists(item.get("guid", "")):
                skipped_count += 1
            else:
                failed_count += 1

        self.logger.info(f"Successfully stored {len(stored_items)} items")
        if skipped_count > 0:
            self.logger.info(f"Skipped {skipped_count} existing items")
        if failed_count > 0:
            self.logger.warning(f"Failed to store {failed_count} items")

        return stored_items

    def update_processing_status(
        self, guid: str, new_status: Literal["fetched", "summarized", "distributed"]
    ):
        """
        Update the processing status of an item in DynamoDB.

        Args:
            guid: The unique identifier of the item
            new_status: New processing status ('fetched', 'summarized', 'distributed')
        """
        try:
            self.table.update_item(
                Key={"guid": guid},
                UpdateExpression="SET processing_status = :status, last_updated = :updated",
                ExpressionAttributeValues={
                    ":status": new_status,
                    ":updated": datetime.now().isoformat(),
                },
            )
            self.logger.info(f"Updated status for item {guid} to {new_status}")
        except Exception as e:
            self.logger.error(f"Error updating status for item {guid}: {e}")

    def store_processed_summary(self, guid: str, summary_content: str):
        """
        Store a processed summary in S3 and update the item's metadata in DynamoDB.

        Args:
            guid: The unique identifier of the item
            summary_content: The processed summary content
        """
        try:
            # Create path for summary: processed_summaries/{guid}.md
            s3_key = f"processed_summaries/{guid}.md"

            # Upload to S3
            self.s3.put_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key,
                Body=summary_content,
                ContentType="text/markdown",
            )

            # Update DynamoDB metadata
            self.table.update_item(
                Key={"guid": guid},
                UpdateExpression="SET summary_path = :path, processing_status = :status, last_updated = :updated",
                ExpressionAttributeValues={
                    ":path": s3_key,
                    ":status": "summarized",
                    ":updated": datetime.now().isoformat(),
                },
            )

            self.logger.info(f"Stored processed summary for item {guid}")
            return s3_key
        except Exception as e:
            self.logger.error(f"Error storing summary for item {guid}: {e}")
            return None

    def store_daily_update(self, update_id: str, update_content: str):
        """
        Store a daily update in S3 and metadata in DynamoDB.

        Args:
            update_id: Unique identifier for the update (usually a date string)
            update_content: The content of the daily update
        """
        try:
            # Create path for daily update: daily_updates/{update_id}.md
            s3_key = f"daily_updates/{update_id}.md"

            # Upload to S3
            self.s3.put_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key,
                Body=update_content,
                ContentType="text/markdown",
            )

            # Store metadata in DynamoDB (using a different pattern since this is not a content item)
            metadata = {
                "guid": f"daily_update_{update_id}",
                "update_id": update_id,
                "s3_path": s3_key,
                "type": "daily_update",
                "created_date": datetime.now().isoformat(),
            }

            self.table.put_item(Item=metadata)

            self.logger.info(f"Stored daily update {update_id}")
            return s3_key
        except Exception as e:
            self.logger.error(f"Error storing daily update {update_id}: {e}")
            return None

    def get_item_metadata(self, guid: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve item metadata from DynamoDB.

        Args:
            guid: The unique identifier of the item

        Returns:
            Item metadata or None if not found
        """
        try:
            response = self.table.get_item(Key={"guid": guid})
            item = response.get("Item")
            return item
        except Exception as e:
            self.logger.error(f"Error retrieving metadata for item {guid}: {e}")
            return None

    def get_content_from_s3(self, s3_path: str) -> Optional[str]:
        """
        Retrieve content from S3.

        Args:
            s3_path: The S3 path (key) of the content

        Returns:
            The content as a string or None if not found
        """
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket_name, Key=s3_path)
            content = response["Body"].read().decode("utf-8")
            return content
        except Exception as e:
            self.logger.error(f"Error retrieving content from S3 path {s3_path}: {e}")
            return None

    def item_exists(self, guid: str, check_status: Optional[List[str]] = None) -> bool:
        """
        Check if an item with the given GUID exists in DynamoDB and optionally
        if it has any of the specified processing statuses.

        Args:
            guid: The unique identifier of the item
            check_status: Optional list of processing statuses to check for
                          (e.g., ["fetched", "summarized", "distributed"])
                          If None, just checks for existence

        Returns:
            True if item exists (and has specified status if provided), False otherwise
        """
        try:
            response = self.table.get_item(Key={"guid": guid})
            item = response.get("Item")

            # If item doesn't exist, return False
            if not item:
                return False

            # If no status check is requested, just confirm existence
            if not check_status:
                return True

            # Check if the item's status is in the list of statuses to check
            item_status = item.get("processing_status")
            return item_status in check_status

        except Exception as e:
            self.logger.error(f"Error checking existence for item {guid}: {e}")
            # When in doubt, assume it doesn't exist to allow reprocessing
            return False

    def check_and_store_item(
        self, item: Dict[str, Any], skip_if_exists: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Check if item exists before storing it. If it exists and skip_if_exists is True,
        skip processing and return None.

        Args:
            item: The item to check and store
            skip_if_exists: Whether to skip storage if item already exists

        Returns:
            Stored item if it was stored, None if skipped or failed
        """
        guid = item.get("guid", "")
        if not guid:
            # Generate a GUID if one doesn't exist
            guid = str(uuid.uuid4())
            item["guid"] = guid

        # Check if item exists
        if skip_if_exists and self.item_exists(guid):
            self.logger.info(
                f"Item with GUID {guid} already exists, skipping processing"
            )
            return None

        # Item doesn't exist or we're not skipping, proceed with storage
        return self.store_single_item(item)

    def needs_summarization(self, guid: str) -> bool:
        """
        Check if an item needs to be summarized - it exists but hasn't been summarized yet.

        Args:
            guid: The unique identifier of the item

        Returns:
            True if the item exists and has 'fetched' status but not 'summarized', False otherwise
        """
        try:
            response = self.table.get_item(Key={"guid": guid})
            item = response.get("Item")

            # If item doesn't exist, it can't be summarized
            if not item:
                return False

            # Check if the item has 'fetched' status but not 'summarized'
            item_status = item.get("processing_status", "")
            return item_status == "fetched"

        except Exception as e:
            self.logger.error(f"Error checking summarization need for item {guid}: {e}")
            return False

    def get_items_needing_processing(
        self, target_status: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get a list of items that need to be processed to the next stage.
        For example, get items with status 'fetched' that need to be 'summarized'.

        Args:
            target_status: The current status to look for (e.g. 'fetched')
            limit: Maximum number of items to return

        Returns:
            List of items that need processing
        """
        try:
            # Create scan filter for the processing status
            scan_filter = {
                "processing_status": {
                    "AttributeValueList": [target_status],
                    "ComparisonOperator": "EQ",
                }
            }

            # Query items with the specified status
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr("processing_status").eq(
                    target_status
                ),
                Limit=limit,
            )

            items = response.get("Items", [])
            self.logger.info(f"Found {len(items)} items with status '{target_status}'")
            return items

        except Exception as e:
            self.logger.error(f"Error getting items needing processing: {e}")
            return []
