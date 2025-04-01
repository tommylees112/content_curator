# Content Curator

A system for automatically fetching, processing, and curating content from various sources into easy-to-consume summaries and newsletters. The system helps users stay informed by automatically processing and summarizing content from their favorite sources, making it easier to consume large amounts of information efficiently.

## Problem Statement

In today's information-rich world, keeping up with multiple content sources can be overwhelming. Content Curator solves this by:
- Automatically fetching content from RSS/Atom feeds and other sources
- Converting content into clean, readable formats
- Generating concise summaries of varying lengths (brief and detailed)
- Creating curated newsletters that combine the most relevant content
- Managing the entire pipeline from fetching to distribution

## Features

- Fetches content from RSS/Atom feeds
- Converts HTML to Markdown for clean, consistent formatting
- Generates two types of summaries:
  - Brief summaries for quick scanning
  - Standard summaries for detailed reading
- Creates curated newsletters from recent content
- Stores content and metadata in AWS (S3 and DynamoDB)
- Sends notifications to Slack
- Supports both local and serverless (AWS Lambda) deployment

## Architecture

The system is designed with a modular, pipeline-based architecture that emphasizes separation of concerns and clear data contracts between components. Each component has a specific responsibility and interacts with others through well-defined interfaces.

### Core Components

1. **Fetchers (Input Layer)**
   - `RSSFetcher`: Fetches content from RSS feeds
   - Future support planned for Web, API, and other content sources
   - Interface: `fetch() -> List[ContentItem]`

2. **Processors (Transform Layer)**
   - `MarkdownProcessor`: Converts HTML to Markdown
   - Future support planned for PDF, DOC, and other formats
   - Interface: `process(item: ContentItem) -> ContentItem`

3. **Enrichers (Enrichment Layer)**
   - `Summarizer`: Generates content summaries
   - Future support planned for categorization and keyword extraction
   - Interface: `enrich(item: ContentItem) -> ContentItem`

4. **Curators (Output Layer)**
   - `NewsletterCurator`: Creates curated newsletters
   - Future support planned for Slack and email distribution
   - Interface: `curate(items: List[ContentItem]) -> OutputType`

### Storage Layer

- **S3Storage**: Manages content storage
  - Stores raw HTML, markdown, and generated summaries
  - Handles file operations and content retrieval
- **DynamoDBState**: Manages metadata and processing state
  - Tracks item status and processing history
  - Maintains relationships between content items

### Design Principles

1. **Clear Data Contracts**
   - Uses `ContentItem` dataclass throughout the pipeline
   - Each stage enriches the ContentItem with additional data
   - Strict interface definitions between components

2. **Independent Components**
   - Each component only depends on ContentItem, DynamoDBState, and S3Storage
   - No direct dependencies between processing stages
   - Components can be deployed and scaled independently

3. **State-Driven Processing**
   - Pipeline stages query DynamoDBState for items to process
   - No direct passing of items between stages
   - Enables distributed processing and better error handling

4. **Flexible Deployment**
   - Supports both local and serverless architectures
   - Components can be deployed as AWS Lambda functions
   - Easy to extend with new content sources and output formats

## AWS Storage Structure

The system uses AWS for storage:

- **S3 Bucket**: Stores the actual content files
  - `markdown/` - Original markdown files from sources ({GUID}.md)
  - `processed_summaries/` - Generated standard summaries ({GUID}.md)
  - `daily_updates/` - Generated daily updates ({date}.md)
  - `curated/` - Generated newsletters ({datetime}.md)

- **DynamoDB**: Stores metadata
  - Item GUID as the primary key
  - Metadata includes title, source, timestamps, processing status
  - References to file locations in S3

## Directory Structure

```
├── .env.example # Example environment variables
├── .gitignore # Git ignore file
├── README.md # Project overview, setup, deployment instructions
├── pyproject.toml # Project metadata, dependencies
├── data/ # Non-code data files (like URL lists)
│ └── rss_urls.txt # List of RSS feed URLs
├── scripts/ # Utility scripts
│ └── run_local_pipeline.py # Script to run the pipeline locally
├── src/content_curator/ # Main source code package
│ ├── config.py # Application configuration
│ ├── core/ # Core logic, interfaces, shared utilities
│ ├── fetchers/ # Module for different data fetchers
│ ├── processors/ # Module for content processing logic
│ │ └── summarizers/ # Module for content summarization
│ │ ├── summarizer.py # Main summarizer class
│ │ ├── standard_summary.txt # Prompt for standard summary
│ │ └── brief_summary.txt # Prompt for brief summary
│ ├── distributors/ # Module for sending content out
│ ├── storage/ # Module for interacting with data stores
│ ├── state/ # Module for managing processing state
│ └── handlers/ # Entry points for cloud functions/services
```

## Installation

1. Clone the repository
2. Set up a Python 3.9+ environment
3. Install dependencies:
   ```bash
   pip install -e .
   ```
   For development:
   ```bash
   pip install -e ".[dev]"
   ```

## Infrastructure Setup

This application requires AWS resources to run. There are two ways to set up the required infrastructure:

### Option 1: Using Terraform (Recommended)

1. Navigate to the Terraform directory:
   ```bash
   cd terraform
   ```

2. Initialize and apply Terraform:
   ```bash
   terraform init
   terraform apply
   ```

3. After running Terraform, update your `.env` file with the output values.

### Option 2: Manual Setup

If you prefer to set up resources manually:

1. Create an S3 bucket with the following structure:
   - `markdown/` folder
   - `processed_summaries/` folder
   - `daily_updates/` folder

2. Create a DynamoDB table with:
   - Table name: content-curator-metadata (or your preferred name)
   - Partition key: "guid" (type: String)

## Configuration

1. Copy `.env.example` to `.env`
2. Configure settings in `.env`:
   ```
   # AWS Configuration
   AWS_S3_BUCKET_NAME=your-bucket-name
   AWS_DYNAMODB_TABLE_NAME=your-table-name
   AWS_REGION=your-region
   AWS_ACCESS_KEY_ID=your-access-key
   AWS_SECRET_ACCESS_KEY=your-secret-key
   
   # Slack Configuration (optional)
   SLACK_WEBHOOK_URL=your-slack-webhook-url
   
   # Application Configuration
   ENVIRONMENT=development
   ```

## Usage

### Running Locally

Use the provided script to run the pipeline locally:

```bash
python scripts/run_local_pipeline.py
```

Or, after installing the package:

```bash
content-curator
```

Options:
- `--steps`: Comma-separated list of steps to run (default: "fetch,process,summarize,curate,notify")
- `--batch-size`: Number of items to process in each batch (default: 5)
- `--env-file`: Path to custom .env file

You can also run specific pipeline stages:

```bash
python main.py --fetch --process --summarize --curate
```

Or just run a single stage:

```bash
python main.py --curate  # Only create a newsletter
```

### Deploying to AWS Lambda

The system is designed to work with AWS Lambda for each processing step:

1. `src/content_curator/handlers/rss_fetch_handler.py`: Fetches and processes RSS feeds
2. `src/content_curator/handlers/summarize_handler.py`: Generates summaries
3. `src/content_curator/handlers/curate_handler.py`: Creates newsletters
4. `src/content_curator/handlers/slack_notify_handler.py`: Sends notifications

You can deploy these Lambda functions using AWS SAM, AWS CDK, or other deployment tools.

## AWS Permissions Required

The AWS user needs the following permissions:

- S3 permissions:
  - `s3:HeadBucket`
  - `s3:PutObject`
  - `s3:GetObject`
  - `s3:DeleteObject`
- DynamoDB permissions:
  - `dynamodb:DescribeTable`
  - `dynamodb:PutItem`
  - `dynamodb:GetItem`
  - `dynamodb:UpdateItem`
  - `dynamodb:Scan`

## Development

- Add new fetchers by extending the `Fetcher` base class
- Add new processors by creating new processing modules
- Add new distributors by creating new distributor modules
- Run tests: `pytest`
- Format code: `black src/ tests/`

## License

MIT
