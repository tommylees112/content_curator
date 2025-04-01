import os
import sys
from pathlib import Path

import boto3  # Keep boto3 import for potential direct use or error handling types
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Set page config must be the first Streamlit command
st.set_page_config(layout="wide")  # Use wide layout for better table display

# --- Assuming your project structure allows this import ---
# Adjust the path based on where you place this script relative to your src folder
sys.path.append(str(Path(__file__).parent))
from src.content_curator.storage import DynamoDBState, S3Storage

# --- Configuration & Initialization ---
load_dotenv()

# Get AWS config from environment variables (provide defaults from your main.py)
S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", "content-curator")
DYNAMODB_TABLE_NAME = os.getenv("AWS_DYNAMODB_TABLE_NAME", "content-curator-metadata")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


# Initialize services using Streamlit's caching for efficiency
@st.cache_resource
def get_dynamodb_state():
    """Cached function to get DynamoDBState instance."""
    try:
        return DynamoDBState(
            dynamodb_table_name=DYNAMODB_TABLE_NAME, aws_region=AWS_REGION
        )
    except Exception as e:
        st.error(f"Failed to initialize DynamoDBState: {e}")
        return None


@st.cache_resource
def get_s3_storage():
    """Cached function to get S3Storage instance."""
    try:
        return S3Storage(s3_bucket_name=S3_BUCKET_NAME, aws_region=AWS_REGION)
    except Exception as e:
        st.error(f"Failed to initialize S3Storage: {e}")
        return None


state_manager = get_dynamodb_state()
s3_storage = get_s3_storage()

if not state_manager or not s3_storage:
    st.error(
        "Failed to initialize AWS services. Please check configuration and credentials."
    )
    st.stop()


# --- Data Fetching ---
# Cache the data fetching function to avoid reloading on every interaction
@st.cache_data(ttl=120)  # Cache data for 120 seconds
def fetch_all_metadata(_state_manager: DynamoDBState):
    """Fetches all metadata items using a DynamoDB scan."""
    # Note: scan is okay for MVP on smaller tables, but inefficient for large ones.
    # Consider implementing a query or limiting the scan later.
    try:
        # Using boto3 directly for scan pagination example
        dynamodb = boto3.resource("dynamodb", region_name=_state_manager.aws_region)
        table = dynamodb.Table(_state_manager.dynamodb_table_name)

        scan_kwargs = {}
        items = []
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        st.success(f"Fetched {len(items)} items from DynamoDB.")
        return items
    except ClientError as e:
        st.error(
            f"Error fetching data from DynamoDB (Table: {DYNAMODB_TABLE_NAME}): {e}"
        )
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred during fetch: {e}")
        return []


# --- Streamlit App UI ---
st.title("Content Curator Admin View")

# Instructions in Sidebar
st.sidebar.info("""
**How to run:**
1. Ensure AWS credentials are configured (e.g., via environment variables, `~/.aws/credentials`, or IAM role).
2. Make sure your `.env` file contains `AWS_S3_BUCKET_NAME`, `AWS_DYNAMODB_TABLE_NAME`, `AWS_REGION`.
3. Install dependencies: `pip install streamlit pandas python-dotenv boto3 botocore` (ensure `boto3` and `botocore` match your project needs).
4. Navigate to your project's root directory in the terminal.
5. Run: `streamlit run admin_view.py` (or the path to your script).
""")

# Fetch data
metadata_items = fetch_all_metadata(state_manager)

if not metadata_items:
    st.warning(f"No metadata found in DynamoDB table '{DYNAMODB_TABLE_NAME}'.")
    # Optionally add a button to retry fetching
    if st.button("Retry Fetch"):
        st.cache_data.clear()  # Clear the cache
        st.rerun()  # Rerun the script
    st.stop()

# Create tabs for different views
tab_items, tab_curated = st.tabs(["Items", "Curated"])

# ===== ITEMS TAB =====
with tab_items:
    # Display data in a table
    st.header("DynamoDB Metadata")
    df = pd.DataFrame(metadata_items)

    # Define desired columns and order (handle missing columns gracefully)
    display_columns_ordered = [
        "guid",
        "published_date",
        "title",
        "md_path",
        "source_url",
        "fetch_date",
        "is_fetched",
        "is_processed",
        "is_summarized",
        "is_distributed",
        "summary_path",
        "last_updated",
    ]
    # Filter to columns that actually exist in the DataFrame
    display_columns = [col for col in display_columns_ordered if col in df.columns]
    # Add any remaining columns not in the predefined list
    other_columns = [col for col in df.columns if col not in display_columns]

    # Display the dataframe - allow users to sort by clicking headers
    st.dataframe(df[display_columns + other_columns], use_container_width=True)

    st.divider()

    # --- Item Detail View ---
    st.header("View Item Content")

    # Create a list of options for the selectbox: "Title (GUID)"
    # Handle cases where 'title' might be missing or empty
    def get_item_display_name(item):
        title = item.get("title", "No Title")
        guid = item.get("guid", "No GUID")
        return f"{title} ({guid})"

    item_options = {
        get_item_display_name(item): item.get("guid") for item in metadata_items
    }

    # Add search functionality
    search_term = st.text_input("Search items", "")
    filtered_options = {
        k: v for k, v in item_options.items() if search_term.lower() in k.lower()
    }

    selected_display_name = st.selectbox(
        "Select item to view content:", options=filtered_options.keys()
    )

    if selected_display_name:
        selected_guid = filtered_options[selected_display_name]
        # Find the full selected item data using the GUID
        # Use next() with a default to handle potential (though unlikely) errors
        selected_item_list = [
            item for item in metadata_items if item.get("guid") == selected_guid
        ]

        if not selected_item_list:
            st.error(f"Could not find data for selected GUID: {selected_guid}")
            st.stop()

        selected_item = selected_item_list[0]

        col1, col2 = st.columns(2)  # Create two columns for content

        # Fetch and display Markdown content from S3
        markdown_s3_path = selected_item.get("md_path")
        with col1:
            # HEADING: Markdown
            st.subheader("Processed Markdown")
            if markdown_s3_path:
                with st.spinner(f"Fetching {markdown_s3_path} from S3..."):
                    try:
                        # Use the S3Storage class method
                        markdown_content = s3_storage.get_content(markdown_s3_path)
                        if markdown_content:
                            # Use st.text_area for potentially long markdown that preserves formatting
                            st.text_area(
                                "Markdown Content",
                                markdown_content,
                                height=400,
                                key="md_content",
                            )
                            # Or use st.markdown if rendering is preferred (might hit limits for very large files)
                            # st.markdown(markdown_content, unsafe_allow_html=False)
                        elif markdown_content is None:
                            st.warning(
                                f"Could not retrieve content from S3 path: {markdown_s3_path}. Path might be incorrect or permissions missing."
                            )
                        else:  # Content is likely an empty string
                            st.info(f"Content file at {markdown_s3_path} is empty.")
                    except ClientError as e:
                        st.error(
                            f"AWS Error fetching content ({markdown_s3_path}): {e}"
                        )
                    except Exception as e:
                        st.error(f"Error fetching content ({markdown_s3_path}): {e}")
            else:
                st.info(
                    "No processed markdown S3 path ('md_path') found for this item."
                )

            # HEADING: JSON details
            st.subheader(f"Details for: {selected_item.get('title', selected_guid)}")
            # Display all metadata for the selected item as JSON in a collapsed expander
            with st.expander("View JSON Details", expanded=False):
                st.json(selected_item)

        # Fetch and display Summary content from S3
        summary_s3_path = selected_item.get("summary_path")
        short_summary_path = selected_item.get("short_summary_path")

        with col2:
            # HEADING: summaries
            st.subheader("Summary")

            # Display short summary if available
            if short_summary_path:
                with st.expander("Short Summary", expanded=True):
                    try:
                        short_summary_content = s3_storage.get_content(
                            short_summary_path
                        )
                        if short_summary_content:
                            st.markdown(short_summary_content, unsafe_allow_html=False)
                        else:
                            st.info("Short summary file is empty.")
                    except Exception as e:
                        st.error(f"Error fetching short summary: {e}")

            # Display full summary
            if summary_s3_path:
                with st.spinner(f"Fetching {summary_s3_path} from S3..."):
                    try:
                        # Use the S3Storage class method
                        summary_content = s3_storage.get_content(summary_s3_path)
                        if summary_content:
                            # Or use st.markdown
                            st.markdown(summary_content, unsafe_allow_html=False)
                        elif summary_content is None:
                            st.warning(
                                f"Could not retrieve summary from S3 path: {summary_s3_path}. Path might be incorrect or permissions missing."
                            )
                        else:  # Content is likely an empty string
                            st.info(f"Summary file at {summary_s3_path} is empty.")
                    except ClientError as e:
                        st.error(f"AWS Error fetching summary ({summary_s3_path}): {e}")
                    except Exception as e:
                        st.error(f"Error fetching summary ({summary_s3_path}): {e}")
            else:
                st.info("No summary S3 path ('summary_path') found for this item.")

# ===== CURATED TAB =====
with tab_curated:
    st.header("Curated Content")

    # Fetch curated content files from S3 "curated/" directory
    try:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        curated_files = []

        # List objects with the curated/ prefix
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix="curated/")

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Skip directory entries
                    if not obj["Key"].endswith("/"):
                        curated_files.append(obj["Key"])

        if not curated_files:
            st.info("No curated content found in the curated/ directory.")
        else:
            # Add search functionality for curated content
            curated_search = st.text_input("Search curated content", "")
            filtered_curated = [
                file for file in curated_files if curated_search.lower() in file.lower()
            ]

            if filtered_curated:
                selected_file = st.selectbox(
                    "Select curated content to view:",
                    options=filtered_curated,
                    key="curated_select",
                    format_func=lambda x: x.split("/")[-1],  # Display just the filename
                )

                if selected_file:
                    with st.spinner(f"Loading curated content from {selected_file}..."):
                        try:
                            content = s3_storage.get_content(selected_file)
                            if content:
                                # Display the markdown content
                                st.markdown(content, unsafe_allow_html=False)
                            else:
                                st.warning("No content found at the specified path.")
                        except Exception as e:
                            st.error(f"Error loading curated content: {e}")
            else:
                st.info("No matching curated files found.")

    except Exception as e:
        st.error(f"Error accessing curated content directory: {e}")
        st.exception(e)
