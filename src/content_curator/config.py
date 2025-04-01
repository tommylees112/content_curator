import os
from pathlib import Path
from typing import Any, List

import yaml
from dotenv import load_dotenv


class Config:
    """Configuration manager for the content curator application."""

    def __init__(self):
        """Initialize the configuration manager."""
        # Load environment variables
        load_dotenv()

        # Get the project root directory
        self.project_root = Path(__file__).parent.parent.parent

        # Load config file
        config_path = self.project_root / "config.yaml"
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Override config values with environment variables if they exist
        self._override_from_env()

    def _override_from_env(self) -> None:
        """Override configuration values with environment variables if they exist."""
        # AWS Configuration
        if os.getenv("AWS_REGION"):
            self.config["aws"]["region"] = os.getenv("AWS_REGION")
        if os.getenv("AWS_S3_BUCKET_NAME"):
            self.config["aws"]["s3"]["bucket_name"] = os.getenv("AWS_S3_BUCKET_NAME")
        if os.getenv("AWS_DYNAMODB_TABLE_NAME"):
            self.config["aws"]["dynamodb"]["table_name"] = os.getenv(
                "AWS_DYNAMODB_TABLE_NAME"
            )

    def get(self, *keys: str, default: Any = None) -> Any:
        """Get a configuration value using dot notation."""
        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
            else:
                return default
        return value

    @property
    def aws_region(self) -> str:
        """Get AWS region."""
        return self.get("aws", "region", default="us-east-1")

    @property
    def s3_bucket_name(self) -> str:
        """Get the S3 bucket name."""
        return self.get("aws", "s3", "bucket_name", default="content-curator")

    @property
    def dynamodb_table_name(self) -> str:
        """Get DynamoDB table name."""
        return self.get(
            "aws", "dynamodb", "table_name", default="content-curator-metadata"
        )

    @property
    def log_file(self) -> str:
        """Get log file path."""
        return self.get("logging", "file", default="content_curator.log")

    @property
    def log_rotation(self) -> str:
        """Get log rotation size."""
        return self.get("logging", "rotation", default="10 MB")

    @property
    def log_retention(self) -> int:
        """Get log retention count."""
        return self.get("logging", "retention", default=5)

    @property
    def log_level(self) -> str:
        """Get log level."""
        return self.get("logging", "level", default="DEBUG")

    @property
    def log_format(self) -> str:
        """Get log format."""
        return self.get(
            "logging",
            "format",
            default="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        )

    @property
    def default_most_recent(self) -> int:
        """Get default number of most recent items."""
        return self.get("pipeline", "default_most_recent", default=10)

    @property
    def rss_default_max_items(self) -> int:
        """Get RSS default maximum items."""
        return self.get("rss", "default_max_items", default=5)

    @property
    def rss_url_file(self) -> str:
        """Get RSS URL file path."""
        return self.get("rss", "rss_url_file", default="data/rss_urls.txt")

    @property
    def summarizer_model_name(self) -> str:
        """Get summarizer model name."""
        return self.get("summarizer", "model_name", default="gemini-1.5-flash")

    @property
    def default_summary_types(self) -> List[str]:
        """Get default summary types."""
        return self.get("summarizer", "default_summary_types", default=["brief"])

    @property
    def curator_content_summary_types(self) -> List[str]:
        """Get the types of summaries to include in newsletters."""
        return self.get("curator", "content_summary_types", default=["brief"])


# Create a global config instance
config = Config()
