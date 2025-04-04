from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

import boto3
from botocore.exceptions import ClientError
from loguru import logger

from src.content_curator.models import ContentItem


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

    def store_item(self, item: ContentItem) -> bool:
        """
        Store ContentItem in DynamoDB.

        Args:
            item: The ContentItem to store

        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert ContentItem to dictionary for DynamoDB
            item_dict = item.to_dict()

            self.table.put_item(Item=item_dict)
            self.logger.info(f"Stored item with GUID: {item.guid}")
            return True
        except Exception as e:
            self.logger.error(f"Error storing item: {e}")
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

    def get_item(self, guid: str) -> Optional[ContentItem]:
        """
        Retrieve ContentItem from DynamoDB.

        Args:
            guid: The unique identifier of the item

        Returns:
            ContentItem or None if not found
        """
        try:
            response = self.table.get_item(Key={"guid": guid})
            item_dict = response.get("Item")

            if not item_dict:
                return None

            # Convert dictionary to ContentItem
            return ContentItem.from_dict(item_dict)
        except Exception as e:
            self.logger.error(f"Error retrieving item {guid}: {e}")
            return None

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

    def get_items_by_status_paths(
        self,
        html_path_exists: Optional[bool] = None,
        md_path_exists: Optional[bool] = None,
        summary_path_exists: Optional[bool] = None,
        has_newsletters: Optional[bool] = None,
        limit: int = 100,
        as_content_items: bool = True,
    ) -> Union[List[Dict[str, Any]], List[ContentItem]]:
        """
        Get items based on their processing status paths.

        Args:
            html_path_exists: Filter for items with html_path
            md_path_exists: Filter for items with md_path
            summary_path_exists: Filter for items with summary_path
            has_newsletters: Filter for items with non-empty newsletters list
            limit: Maximum number of items to return
            as_content_items: If True, return as ContentItem objects instead of dictionaries

        Returns:
            List of matching items as ContentItem objects (or dictionaries if as_content_items=False)
        """
        try:
            # Build the filter expression based on provided flags
            filter_expression = None

            if html_path_exists is not None:
                if html_path_exists:
                    filter_expression = boto3.dynamodb.conditions.Attr(
                        "html_path"
                    ).exists()
                else:
                    filter_expression = ~boto3.dynamodb.conditions.Attr(
                        "html_path"
                    ).exists()

            if md_path_exists is not None:
                new_condition = (
                    boto3.dynamodb.conditions.Attr("md_path").exists()
                    if md_path_exists
                    else ~boto3.dynamodb.conditions.Attr("md_path").exists()
                )
                filter_expression = (
                    new_condition
                    if filter_expression is None
                    else filter_expression & new_condition
                )

            if summary_path_exists is not None:
                new_condition = (
                    boto3.dynamodb.conditions.Attr("summary_path").exists()
                    if summary_path_exists
                    else ~boto3.dynamodb.conditions.Attr("summary_path").exists()
                )
                filter_expression = (
                    new_condition
                    if filter_expression is None
                    else filter_expression & new_condition
                )

            if has_newsletters is not None:
                if has_newsletters:
                    # Check if newsletters exists and is not empty
                    new_condition = boto3.dynamodb.conditions.Attr(
                        "newsletters"
                    ).exists() & boto3.dynamodb.conditions.Attr(
                        "newsletters"
                    ).size().gt(0)
                else:
                    # Check if newsletters doesn't exist or is empty
                    not_exists = ~boto3.dynamodb.conditions.Attr("newsletters").exists()
                    exists_but_empty = boto3.dynamodb.conditions.Attr(
                        "newsletters"
                    ).exists() & boto3.dynamodb.conditions.Attr(
                        "newsletters"
                    ).size().eq(0)
                    new_condition = not_exists | exists_but_empty

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

            # Convert to ContentItem objects if requested
            if as_content_items and items:
                return [ContentItem.from_dict(item) for item in items]

            return items

        except Exception as e:
            self.logger.error(f"Error getting items by status paths: {e}")
            return []

    def get_items_for_stage(
        self,
        stage: Literal["process", "summarize", "curate"],
        specific_id: Optional[str] = None,
        overwrite_flag: bool = False,
        limit: int = 100,
    ) -> List[ContentItem]:
        """
        Get items that need to be processed for a specific stage.
        This is a helper method to centralize the logic for getting items that need processing.

        Args:
            stage: The stage to get items for
            specific_id: Optional specific item ID to process
            overwrite_flag: Whether to overwrite existing content
            limit: Maximum number of items to return

        Returns:
            List of ContentItem objects that need processing
        """
        self.logger.debug(
            f"Getting items for stage '{stage}' with parameters: specific_id={specific_id}, overwrite_flag={overwrite_flag}, limit={limit}"
        )

        if specific_id:
            self.logger.debug(f"Processing specific item: {specific_id}")
            item = self.get_item(specific_id)
            if item:
                self.logger.debug(f"Found specific item: {item.guid}")
                return [item]
            return []

        # Get items based on stage
        if stage == "process":
            # Get items that have HTML content but no markdown
            items = self.get_items_by_status_paths(
                html_path_exists=True,
                md_path_exists=False,
                limit=limit,
                as_content_items=True,
            )
            self.logger.debug(
                f"Found {len(items)} items needing initial processing (HTML exists but no markdown)"
            )
        elif stage == "summarize":
            # Get items that have markdown but no summary
            items = self.get_items_by_status_paths(
                md_path_exists=True,
                summary_path_exists=False,
                limit=limit,
                as_content_items=True,
            )
            self.logger.debug(
                f"Found {len(items)} items needing initial summarization (Markdown exists but no summary)"
            )
        else:  # curate
            # Get items that have summaries but haven't been curated
            items = self.get_items_by_status_paths(
                summary_path_exists=True,
                has_newsletters=False,
                limit=limit,
                as_content_items=True,
            )
            self.logger.debug(
                f"Found {len(items)} items needing initial curation (Summary exists but no newsletters)"
            )

        initial_count = len(items)
        remaining_limit = limit - initial_count
        self.logger.debug(
            f"After initial query: {initial_count} items found, {remaining_limit} slots remaining in limit"
        )

        # If overwrite is enabled and we haven't hit the limit, also get items that already have the target content
        if overwrite_flag and remaining_limit > 0:
            if stage == "process":
                # Get items that have both HTML and markdown
                existing_items = self.get_items_by_status_paths(
                    html_path_exists=True,
                    md_path_exists=True,
                    limit=remaining_limit,  # Only get up to remaining limit
                    as_content_items=True,
                )
                self.logger.debug(
                    f"Found {len(existing_items)} existing items to overwrite for processing (limited by remaining slots: {remaining_limit})"
                )
                items.extend(existing_items)
            elif stage == "summarize":
                # Get items that have both markdown and summary
                existing_items = self.get_items_by_status_paths(
                    md_path_exists=True,
                    summary_path_exists=True,
                    limit=remaining_limit,  # Only get up to remaining limit
                    as_content_items=True,
                )
                self.logger.debug(
                    f"Found {len(existing_items)} existing items to overwrite for summarization (limited by remaining slots: {remaining_limit})"
                )
                items.extend(existing_items)

        # Log details about each item being returned
        self.logger.debug(
            f"Total items being returned: {len(items)} (limit was {limit})"
        )
        for item in items:
            self.logger.debug(
                f"Item {item.guid}: HTML={bool(item.html_path)}, MD={bool(item.md_path)}, Summary={bool(item.summary_path)}"
            )

        return items

    def get_items_needing_summarization(
        self, limit: int = 10, as_content_items: bool = True
    ) -> Union[List[Dict[str, Any]], List[ContentItem]]:
        """
        Get items that need summarization (processed, worth summarizing, not summarized yet).

        Args:
            limit: Maximum number of items to return
            as_content_items: If True, return results as ContentItem objects

        Returns:
            List of items that need summarization
        """
        try:
            # Build filter expression for:
            # 1. Items that have md_path (processed)
            # 2. Items that don't have summary_path (not summarized)
            # 3. Items that are worth summarizing (to_be_summarized = True)
            filter_expression = boto3.dynamodb.conditions.Attr("md_path").exists()
            filter_expression = (
                filter_expression
                & ~boto3.dynamodb.conditions.Attr("summary_path").exists()
            )

            # Only include items explicitly marked as worth summarizing
            # or items that haven't been evaluated yet (to_be_summarized is null)
            worth_summarizing_condition = boto3.dynamodb.conditions.Attr(
                "to_be_summarized"
            ).eq(True)
            not_evaluated_condition = boto3.dynamodb.conditions.Attr(
                "to_be_summarized"
            ).not_exists()
            worth_condition = worth_summarizing_condition | not_evaluated_condition

            filter_expression = filter_expression & worth_condition

            # Execute scan with the filter
            response = self.table.scan(
                FilterExpression=filter_expression,
                Limit=limit,
            )

            items = response.get("Items", [])
            self.logger.info(f"Found {len(items)} items that need summarization")

            # Convert to ContentItem objects if requested
            if as_content_items and items:
                return [ContentItem.from_dict(item) for item in items]

            return items

        except Exception as e:
            self.logger.error(f"Error getting items needing summarization: {e}")
            return []

    def update_item(self, item: ContentItem, overwrite_flag: bool = False) -> bool:
        """
        Update an item in DynamoDB with the current state of a ContentItem.
        Preserves existing fields by merging the new item with the existing one,
        unless overwrite_flag is True.

        Args:
            item: The ContentItem to update
            overwrite_flag: If True, completely overwrite the item instead of merging

        Returns:
            True if successful, False otherwise
        """
        try:
            # First, get the existing item to ensure we preserve all fields
            existing_item = self.get_item(item.guid)

            if existing_item and not overwrite_flag:
                # Create a merged item by starting with existing data
                # and then updating with the new data (only non-None fields)
                merged_dict = existing_item.to_dict()

                # Convert new item to dictionary
                new_dict = item.to_dict()

                # Merge fields, only updating those present in the new item
                # and skipping 'guid' which is the primary key
                for key, value in new_dict.items():
                    if key != "guid" and value is not None:
                        merged_dict[key] = value

                # Always update the last_updated timestamp
                merged_dict["last_updated"] = datetime.now().isoformat()

                # Log what we're merging for debugging
                self.logger.debug(
                    f"Merging item {item.guid} - preserving existing paths: "
                    f"md_path={merged_dict.get('md_path')}, "
                    f"summary_path={merged_dict.get('summary_path')}, "
                    f"short_summary_path={merged_dict.get('short_summary_path')}"
                )

                # Remove guid which is the primary key - we'll use it separately
                guid = merged_dict.pop("guid")

                # Update the item using the existing update_metadata method
                return self.update_metadata(guid, merged_dict)
            else:
                # If no existing item or overwrite is enabled, just update with the new item
                item_dict = item.to_dict()
                guid = item_dict.pop("guid")
                return self.update_metadata(guid, item_dict)

        except Exception as e:
            self.logger.error(f"Error updating item {item.guid}: {e}")
            return False

    def update_metadata(self, guid: str, updates: Dict[str, Any]) -> bool:
        """
        Update metadata fields for an item.

        Args:
            guid: The unique identifier of the item
            updates: Dictionary of fields and values to update

        Returns:
            True if successful, False otherwise

        Note:
            This method is used internally by update_item.
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
                self.logger.debug(f"Updated metadata for item {guid}: {update_summary}")
            else:
                self.logger.debug(f"Updated metadata for item {guid}")

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

    def get_all_items(
        self, as_content_items: bool = True
    ) -> Union[List[Dict[str, Any]], List[ContentItem]]:
        """
        Get all items from the DynamoDB table.

        Args:
            as_content_items: If True, return results as ContentItem objects instead of dictionaries
                             (Default is now True to encourage use of ContentItem objects)

        Returns:
            List of all items in the table as ContentItem objects (or dictionaries if as_content_items=False)
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

            # Convert to ContentItem objects if requested
            if as_content_items and items:
                return [ContentItem.from_dict(item) for item in items]

            return items
        except Exception as e:
            self.logger.error(f"Error retrieving all items: {e}")
            return []
