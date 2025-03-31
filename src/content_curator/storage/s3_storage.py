from typing import Optional

import boto3
from botocore.client import BaseClient
from loguru import logger


class S3Storage:
    """
    Handles S3 storage operations for content curation, specifically storing and retrieving
    markdown files and other content.
    """

    def __init__(
        self,
        s3_bucket_name: str,
        aws_region: str = "us-east-1",
    ) -> None:
        """
        Initialize S3 storage with bucket name.

        Args:
            s3_bucket_name: Name of the S3 bucket
            aws_region: AWS region to use
        """
        self.s3_bucket_name: str = s3_bucket_name
        self.aws_region: str = aws_region

        # Initialize S3 client
        self.s3: BaseClient = boto3.client("s3", region_name=aws_region)
        self.logger = logger

    def check_resources_exist(self) -> bool:
        """
        Check if necessary S3 bucket exists.
        Returns True if the bucket exists, False otherwise.
        """
        try:
            self.s3.head_bucket(Bucket=self.s3_bucket_name)
            self.logger.info(f"S3 bucket {self.s3_bucket_name} exists")
            return True
        except Exception as e:
            self.logger.error(
                f"S3 bucket {self.s3_bucket_name} does not exist or is not accessible: {e}"
            )
            return False

    def store_content(
        self, key: str, content: str, content_type: str = "text/markdown"
    ) -> bool:
        """
        Store content in S3.

        Args:
            key: S3 key (path) to store the content at
            content: The content to store
            content_type: The content type (MIME type)

        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3.put_object(
                Bucket=self.s3_bucket_name,
                Key=key,
                Body=content,
                ContentType=content_type,
            )
            self.logger.info(f"Stored content at S3 path: {key}")
            return True
        except Exception as e:
            self.logger.error(f"Error storing content at S3 path {key}: {e}")
            return False

    def get_content(self, key: str) -> Optional[str]:
        """
        Retrieve content from S3.

        Args:
            key: The S3 key (path) of the content

        Returns:
            The content as a string or None if not found
        """
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket_name, Key=key)
            content: str = response["Body"].read().decode("utf-8")
            return content
        except Exception as e:
            self.logger.error(f"Error retrieving content from S3 path {key}: {e}")
            return None
