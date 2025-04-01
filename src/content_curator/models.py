from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

SummaryType = Literal["standard", "brief"]


@dataclass
class ContentItem:
    """Represents a piece of content as it moves through the pipeline."""

    # Core identifiers and fetched metadata
    guid: str  # Unique identifier (often URL hash or feed GUID)
    link: str  # Direct link to the content
    title: Optional[str] = None
    published_date: Optional[str] = (
        None  # Consider parsing to datetime if needed consistently
    )
    fetch_date: Optional[str] = field(
        default_factory=lambda: datetime.now().isoformat()
    )
    source_url: Optional[str] = None  # The URL of the RSS feed it came from

    # Content quality flags (not processing state)
    is_paywall: Optional[bool] = None  # Determined during processing
    to_be_summarized: Optional[bool] = None  # Determined during processing

    # Content storage references (Paths/Keys in S3)
    html_path: Optional[str] = (
        None  # Path to the raw HTML content - indicates item is fetched
    )
    md_path: Optional[str] = (
        None  # Path to the processed Markdown content - indicates item is processed
    )
    summary_path: Optional[str] = (
        None  # Path to the standard summary - indicates item is summarized
    )
    short_summary_path: Optional[str] = None  # Path to the brief summary

    # Content (loaded in memory when needed, not typically stored in the dataclass long-term)
    # These are often loaded just-in-time by the stage that needs them
    html_content: Optional[str] = None
    markdown_content: Optional[str] = None
    summary: Optional[str] = None
    short_summary: Optional[str] = None

    # Curation/Distribution info
    newsletters: List[str] = field(
        default_factory=list
    )  # List of newsletter IDs it was included in - non-empty list indicates item is distributed

    # Timestamps
    last_updated: Optional[str] = field(
        default_factory=lambda: datetime.now().isoformat()
    )

    def __post_init__(self):
        # Ensure guid and link are always present after initialization
        if not self.guid:
            raise ValueError("ContentItem must have a guid.")
        if not self.link:
            raise ValueError("ContentItem must have a link.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert the ContentItem to a dictionary for storage, omitting None values and
        in-memory content fields that should not be stored in the database."""
        # Use asdict to convert to dictionary
        item_dict = asdict(self)

        # Remove in-memory content fields that shouldn't be stored
        content_fields = [
            "html_content",
            "markdown_content",
            "summary",
            "short_summary",
        ]
        for field in content_fields:
            if field in item_dict:
                item_dict.pop(field)

        # Remove None values to save storage space
        return {k: v for k, v in item_dict.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContentItem":
        """Create a ContentItem instance from a dictionary, handling missing fields."""
        # Filter the dictionary to only include fields that are part of the dataclass
        valid_fields = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}

        return cls(**filtered_data)

    def update(self, updates: Dict[str, Any]) -> None:
        """Update the ContentItem with the provided updates dictionary."""
        for key, value in updates.items():
            if hasattr(self, key):
                setattr(self, key, value)

        # Always update the last_updated timestamp
        self.last_updated = datetime.now().isoformat()
