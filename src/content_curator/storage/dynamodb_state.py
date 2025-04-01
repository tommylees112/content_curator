from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import boto3
from botocore.exceptions import ClientError
from loguru import logger


class DynamoDBState:
    """
    Handles DynamoDB operations for managing content curation state and metadata.
    """

    def __init__(
        self,
        dynamodb_table_name: str,
        aws_region: str = "us-east-1",
    ):
        """
        Initialize DynamoDB state manager.

        Args:
            dynamodb_table_name: Name of the DynamoDB table
            aws_region: AWS region to use
        """
        self.dynamodb_table_name = dynamodb_table_name
        self.aws_region = aws_region

        # Initialize DynamoDB resource
        self.dynamodb = boto3.resource("dynamodb", region_name=aws_region)
        self.table = self.dynamodb.Table(dynamodb_table_name)
        self.logger = logger

    def check_resources_exist(self) -> bool:
        """
        Check if necessary DynamoDB table exists.
        Returns True if the table exists, False otherwise.
        """
        try:
            self.table.table_status
            self.logger.info(f"DynamoDB table {self.dynamodb_table_name} exists")
            return True
        except ClientError as e:
            self.logger.error(
                f"DynamoDB table {self.dynamodb_table_name} does not exist or is not accessible: {e}"
            )
            return False

    def store_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Store metadata in DynamoDB.

        Args:
            metadata: The metadata to store

        Returns:
            True if successful, False otherwise
        """
        try:
            self.table.put_item(Item=metadata)
            self.logger.info(
                f"Stored metadata for item with GUID: {metadata.get('guid')}"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error storing metadata: {e}")
            return False

    def get_metadata(self, guid: str) -> Optional[Dict[str, Any]]:
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

    def update_status(
        self,
        guid: str,
        new_status: Literal["fetched", "processed", "summarized", "distributed"],
    ) -> bool:
        """
        Update the processing status of an item.

        Args:
            guid: The unique identifier of the item
            new_status: The new processing status to set

        Returns:
            True if successful, False otherwise
        """
        try:
            # Update the status and last_updated timestamp
            updates = {
                "processing_status": new_status,
                "last_updated": datetime.now().isoformat(),
            }

            return self.update_metadata(guid, updates)

        except Exception as e:
            self.logger.error(f"Error updating status for item {guid}: {e}")
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

    def get_items_by_status_flags(
        self,
        is_fetched: bool = None,
        is_processed: bool = None,
        is_summarized: bool = None,
        is_distributed: bool = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get items based on their processing status flags.

        Args:
            is_fetched: Filter for fetched status
            is_processed: Filter for processed status
            is_summarized: Filter for summarized status
            is_distributed: Filter for distributed status
            limit: Maximum number of items to return

        Returns:
            List of matching items
        """
        try:
            # Build the filter expression based on provided flags
            filter_expression = None

            if is_fetched is not None:
                filter_expression = boto3.dynamodb.conditions.Attr("is_fetched").eq(
                    is_fetched
                )

            if is_processed is not None:
                new_condition = boto3.dynamodb.conditions.Attr("is_processed").eq(
                    is_processed
                )
                filter_expression = (
                    new_condition
                    if filter_expression is None
                    else filter_expression & new_condition
                )

            if is_summarized is not None:
                new_condition = boto3.dynamodb.conditions.Attr("is_summarized").eq(
                    is_summarized
                )
                filter_expression = (
                    new_condition
                    if filter_expression is None
                    else filter_expression & new_condition
                )

            if is_distributed is not None:
                new_condition = boto3.dynamodb.conditions.Attr("is_distributed").eq(
                    is_distributed
                )
                filter_expression = (
                    new_condition
                    if filter_expression is None
                    else filter_expression & new_condition
                )

            # Execute scan with the filter
            if filter_expression:
                response = self.table.scan(
                    FilterExpression=filter_expression,
                    Limit=limit,
                )
            else:
                response = self.table.scan(Limit=limit)

            items = response.get("Items", [])
            return items

        except Exception as e:
            self.logger.error(f"Error getting items by status flags: {e}")
            return []

    def update_metadata(self, guid: str, updates: Dict[str, Any]) -> bool:
        """
        Update metadata fields for an item.

        Args:
            guid: The unique identifier of the item
            updates: Dictionary of fields and values to update

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the update expression and attribute values
            update_expression = "SET "
            expression_attribute_values = {}

            for key, value in updates.items():
                update_expression += f"{key} = :{key.replace('-', '_')}, "
                expression_attribute_values[f":{key.replace('-', '_')}"] = value

            # Remove the trailing comma and space
            update_expression = update_expression[:-2]

            # Perform the update
            self.table.update_item(
                Key={"guid": guid},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
            )

            # Format a readable update summary, excluding last_updated which changes every time
            update_fields = [
                f"{k}={v}" for k, v in updates.items() if k != "last_updated"
            ]
            if update_fields:
                update_summary = ", ".join(update_fields)
                self.logger.info(f"Updated metadata for item {guid}: {update_summary}")
            else:
                self.logger.info(f"Updated metadata for item {guid}")

            return True

        except Exception as e:
            self.logger.error(f"Error updating metadata for item {guid}: {e}")
            return False

    def delete_item(self, guid: str) -> bool:
        """
        Delete an item from DynamoDB by its GUID.

        Args:
            guid: The unique identifier of the item to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            self.table.delete_item(Key={"guid": guid})
            self.logger.info(f"Deleted item with GUID: {guid}")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting item {guid}: {e}")
            return False

    def get_all_items(self) -> List[Dict[str, Any]]:
        """
        Get all items from the DynamoDB table.

        Returns:
            List of all items in the table
        """
        try:
            items = []
            scan_kwargs = {}
            done = False
            start_key = None

            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = self.table.scan(**scan_kwargs)
                items.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None

            self.logger.info(f"Retrieved {len(items)} items from DynamoDB")
            return items
        except Exception as e:
            self.logger.error(f"Error retrieving all items: {e}")
            return []
