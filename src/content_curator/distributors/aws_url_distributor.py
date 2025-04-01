from typing import Optional

import boto3
from botocore.exceptions import ClientError
from loguru import logger

from src.content_curator.config import config
from src.content_curator.storage.s3_storage import S3Storage


class AWSURLDistributor:
    """
    Distributes content by generating a pre-signed URL for an S3 object.
    Can convert Markdown to HTML and distribute the HTML version.
    """

    def __init__(
        self,
        s3_storage: S3Storage,
        bucket_name: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        """
        Initializes the distributor with the S3 storage and configuration.

        Args:
            s3_storage: The S3 storage manager.
            bucket_name: The name of the S3 bucket. If None, will use the configured bucket.
            region_name: The AWS region where the bucket is located. If None,
                        will use the configured region.
        """
        self.s3_storage = s3_storage
        self.bucket_name = bucket_name or config.s3_bucket_name
        self.region_name = region_name or config.aws_region

        if not self.bucket_name:
            raise ValueError("S3 bucket name must be provided.")

        # Create a dedicated client for presigned URLs
        self.s3_client = boto3.client("s3", region_name=self.region_name)

        # Initialize the HTML converter - import locally to avoid circular imports
        from src.content_curator.distributors.html_converter import HTMLConverter

        self.html_converter = HTMLConverter(s3_storage, bucket_name, region_name)

        logger.info(f"Initialized AWSURLDistributor for bucket: {self.bucket_name}")

    def distribute(
        self, s3_key: str = "curated/latest_standard.md", expiration: int = 3600
    ) -> Optional[str]:
        """
        Generates a pre-signed URL for the specified S3 object and logs it.

        Args:
            s3_key: The key (path) of the object in the S3 bucket. Defaults to "curated/latest_standard.md".
            expiration: The time in seconds for which the pre-signed URL is valid. Defaults to 3600 (1 hour).

        Returns:
            The pre-signed URL as a string, or None if an error occurred.
        """
        try:
            # Check if the object exists first (using the s3_storage to maintain consistency)
            if not self.s3_storage.object_exists(s3_key):
                logger.warning(
                    f"Object s3://{self.bucket_name}/{s3_key} does not exist."
                )
                return None

            # Generate the presigned URL
            response = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": s3_key},
                ExpiresIn=expiration,
            )
            logger.info(
                f"Generated pre-signed URL for s3://{self.bucket_name}/{s3_key}"
            )
            logger.debug(f"URL: {response}")
            return response
        except ClientError as e:
            logger.error(
                f"Failed to generate pre-signed URL for s3://{self.bucket_name}/{s3_key}: {e}"
            )
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None

    def distribute_as_html(
        self, s3_key: str = "curated/latest_standard.md", expiration: int = 3600
    ) -> Optional[str]:
        """
        Converts the Markdown content to HTML, stores it in S3, and generates a pre-signed URL.
        Uses the HTMLConverter class for the conversion.

        Args:
            s3_key: The key (path) of the Markdown object in the S3 bucket. Defaults to "curated/latest_standard.md".
            expiration: The time in seconds for which the pre-signed URL is valid. Defaults to 3600 (1 hour).

        Returns:
            The pre-signed URL for the HTML file as a string, or None if an error occurred.
        """
        try:
            # Use the HTMLConverter to convert and store the HTML
            html_key = self.html_converter.convert(s3_key)

            if not html_key:
                logger.warning(f"Failed to convert markdown to HTML for {s3_key}")
                return None

            # Generate the presigned URL for the HTML file
            response = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": html_key},
                ExpiresIn=expiration,
            )
            logger.info(
                f"Generated pre-signed URL for HTML version at s3://{self.bucket_name}/{html_key}"
            )
            logger.debug(f"URL: {response}")
            return response
        except ClientError as e:
            logger.error(
                f"Failed to generate pre-signed URL for HTML version of s3://{self.bucket_name}/{s3_key}: {e}"
            )
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None


if __name__ == "__main__":
    # Initialize the S3 storage
    s3_storage = S3Storage(
        s3_bucket_name=config.s3_bucket_name,
        aws_region=config.aws_region,
    )

    # Initialize the distributor
    distributor = AWSURLDistributor(s3_storage=s3_storage)

    html_url = distributor.distribute_as_html()
    if html_url:
        logger.info(
            "Successfully generated pre-signed URL for HTML version of standard summary"
        )
