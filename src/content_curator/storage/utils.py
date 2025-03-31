from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key
from loguru import logger


def query_dynamodb(
    table_name: str,
    aws_region: str = "us-east-1",
    key_condition: Optional[Dict[str, Any]] = None,
    filter_expression: Optional[Dict[str, Any]] = None,
    index_name: Optional[str] = None,
    limit: Optional[int] = None,
    scan_forward: bool = True,
    time_range_days: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Query or scan DynamoDB table with flexible filtering options.

    Args:
        table_name: Name of the DynamoDB table
        aws_region: AWS region where the table is located
        key_condition: Dictionary for primary key condition with format {"attribute_name": "value"}
                       or {"attribute_name": {"operator": "value"}}
                       where operator can be =, <, <=, >, >=, begins_with
        filter_expression: Dictionary of attribute filters with same format as key_condition
        index_name: Optional secondary index name to query
        limit: Maximum number of items to return
        scan_forward: Sort direction (True for ascending, False for descending)
        time_range_days: If provided, adds a filter for items updated within the last N days
                        (requires a 'last_updated' attribute in ISO format)

    Returns:
        List of items matching the query/scan criteria
    """
    dynamodb = boto3.resource("dynamodb", region_name=aws_region)
    table = dynamodb.Table(table_name)

    # Determine if we should use query or scan
    use_query = key_condition is not None

    try:
        # Build key condition expression for query
        if use_query:
            key_condition_exp = None
            for attr_name, condition in key_condition.items():
                if isinstance(condition, dict):
                    # Handle operators
                    for op, value in condition.items():
                        if op == "=":
                            key_expr = Key(attr_name).eq(value)
                        elif op == "<":
                            key_expr = Key(attr_name).lt(value)
                        elif op == "<=":
                            key_expr = Key(attr_name).lte(value)
                        elif op == ">":
                            key_expr = Key(attr_name).gt(value)
                        elif op == ">=":
                            key_expr = Key(attr_name).gte(value)
                        elif op == "begins_with":
                            key_expr = Key(attr_name).begins_with(value)
                        else:
                            raise ValueError(f"Unsupported key operation: {op}")
                else:
                    # Simple equality condition
                    key_expr = Key(attr_name).eq(condition)

                # Combine conditions with AND
                if key_condition_exp is None:
                    key_condition_exp = key_expr
                else:
                    key_condition_exp = key_condition_exp & key_expr

        # Build filter expression
        filter_exp = None
        if filter_expression:
            for attr_name, condition in filter_expression.items():
                if isinstance(condition, dict):
                    # Handle operators
                    for op, value in condition.items():
                        if op == "=":
                            expr = Attr(attr_name).eq(value)
                        elif op == "<":
                            expr = Attr(attr_name).lt(value)
                        elif op == "<=":
                            expr = Attr(attr_name).lte(value)
                        elif op == ">":
                            expr = Attr(attr_name).gt(value)
                        elif op == ">=":
                            expr = Attr(attr_name).gte(value)
                        elif op == "begins_with":
                            expr = Attr(attr_name).begins_with(value)
                        elif op == "contains":
                            expr = Attr(attr_name).contains(value)
                        else:
                            raise ValueError(f"Unsupported filter operation: {op}")
                else:
                    # Simple equality condition
                    expr = Attr(attr_name).eq(condition)

                # Combine expressions with AND
                if filter_exp is None:
                    filter_exp = expr
                else:
                    filter_exp = filter_exp & expr

        # Add time range filter if specified
        if time_range_days is not None:
            cutoff_date = (datetime.now() - timedelta(days=time_range_days)).isoformat()
            time_expr = Attr("last_updated").gte(cutoff_date)

            if filter_exp is None:
                filter_exp = time_expr
            else:
                filter_exp = filter_exp & time_expr

        # Execute query or scan based on provided parameters
        kwargs = {}
        if index_name:
            kwargs["IndexName"] = index_name
        if limit:
            kwargs["Limit"] = limit

        if use_query:
            kwargs["ScanIndexForward"] = scan_forward
            kwargs["KeyConditionExpression"] = key_condition_exp
            if filter_exp:
                kwargs["FilterExpression"] = filter_exp
            response = table.query(**kwargs)
        else:
            if filter_exp:
                kwargs["FilterExpression"] = filter_exp
            response = table.scan(**kwargs)

        items = response.get("Items", [])

        # Handle pagination for larger result sets
        while "LastEvaluatedKey" in response and (limit is None or len(items) < limit):
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

            if use_query:
                response = table.query(**kwargs)
            else:
                response = table.scan(**kwargs)

            items.extend(response.get("Items", []))

            # Respect the limit if provided
            if limit and len(items) >= limit:
                items = items[:limit]
                break

        logger.info(f"Retrieved {len(items)} items from DynamoDB table {table_name}")
        return items

    except Exception as e:
        logger.error(f"Error querying/scanning DynamoDB table {table_name}: {e}")
        return []


def get_recent_content_metadata(
    table_name: str,
    days: int = 7,
    status: Optional[str] = None,
    aws_region: str = "us-east-1",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Get metadata for content items processed within the specified time period.

    Args:
        table_name: Name of the DynamoDB table
        days: Number of days to look back
        status: Filter by processing status (e.g., "fetched", "summarized")
        aws_region: AWS region where the table is located
        limit: Maximum number of items to return

    Returns:
        List of metadata items for recently processed content
    """
    filter_expression = {}
    if status:
        filter_expression["processing_status"] = status

    return query_dynamodb(
        table_name=table_name,
        aws_region=aws_region,
        filter_expression=filter_expression,
        time_range_days=days,
        limit=limit,
        scan_forward=False,  # Return newest items first
    )


def get_item_by_guid(
    table_name: str, guid: str, aws_region: str = "us-east-1"
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific item by its GUID.

    Args:
        table_name: Name of the DynamoDB table
        guid: The global unique identifier for the item
        aws_region: AWS region where the table is located

    Returns:
        The item metadata or None if not found
    """
    try:
        items = query_dynamodb(
            table_name=table_name,
            aws_region=aws_region,
            key_condition={"guid": guid},
            limit=1,
        )
        return items[0] if items else None
    except Exception as e:
        logger.error(f"Error retrieving item with GUID {guid}: {e}")
        return None


if __name__ == "__main__":
    # Example script to export DynamoDB metadata to CSV
    from datetime import datetime

    import pandas as pd

    # Configuration
    TABLE_NAME = "content-curator-metadata"
    OUTPUT_FILE = f"content_metadata_{datetime.now().strftime('%Y%m%d')}.csv"

    try:
        # Query recent items (last 30 days)
        items = get_recent_content_metadata(
            table_name=TABLE_NAME,
            days=30,
            limit=1000,  # Adjust as needed
        )

        if not items:
            logger.warning("No items found to export")
            exit()

        # Convert to pandas DataFrame
        df = pd.DataFrame(items)

        # Export to CSV
        df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Successfully exported {len(df)} items to {OUTPUT_FILE}")

    except Exception as e:
        logger.error(f"Error exporting metadata to CSV: {e}")
