# TODO
# Content Curator Improvements

## Completed
- [x] Added `ContentItem` dataclass in `src/content_curator/models.py` to represent content items throughout the pipeline
- [x] Updated `DynamoDBState` to use ContentItem with new `store_item`, `get_item`, and `update_item` methods
- [x] Updated `RssFetcher` to return ContentItem objects
- [x] Updated `MarkdownProcessor` to work with ContentItem objects
- [x] Updated `Summarizer` to work with ContentItem objects
- [x] Updated `NewsletterCurator` to work with ContentItem objects
- [x] Updated `main.py` to use ContentItem throughout the pipeline stages
- [x] Updated scripts to use ContentItem objects (`update_guids.py`, `fix_summary_metadata.py`)
- [x] Updated examples to use ContentItem objects (`summarization_process.py`)
- [x] Added `get_items_needing_summarization` method to DynamoDBState to get items that need summarization
- [x] ensure that the fetcher stores the html that is collected in the s3 storage.create a new html_path field in the ContentItem dataclass and column in the dynamodb table. Also rename / update the s3_path to be md_path.
- [x] Clean up original dictionary-based methods after fully validating the ContentItem implementation

## TODO
- [ ] Add comprehensive unit tests for the ContentItem implementation
- [ ] Add docstrings explaining the purpose and lifecycle of ContentItem fields
- [ ] Consider refactoring the pipeline to make each stage more independent:
  - [ ] Extract the `get_items_for_stage()` helper function to centralize item loading logic
  - [ ] Move more stage logic from `main.py` to the respective classes
- [ ] Make configuration more centralized, moving hardcoded values into a config file
- [ ] Prepare for potential serverless architecture:
  - [ ] Create a `functions/` directory with handlers for each pipeline stage
  - [ ] Implement serverless function entry points for each stage
  - [ ] Set up infrastructure as code (e.g., AWS SAM or Serverless Framework config)
- [ ] Improve error handling with specific exception types and retry logic
- [ ] refactor the ContentItem and its uses throughout the pipeline to simply look for summary_path and short_summary_path instead of having the boolean flags: is_fetched, is_processed, is_summarized, is_distributed. Processes that use these  should just look at the paths. in all of the fetchers, processors, summarizers and curators. Remove references to the boolean flags. Do not give warnings or deprecations, just make the changes.
- [ ] Only run the short summary stage by default. We can do the full summary in two ways 1) run when the user asks for it 2) run when that item is part of a newsletter / curated into the newsletter. Come up with designs and ways of doing this that maintain the separation of concerns between stages.
- [ ] separation of concernns means that the differnet components do not rely on each other but only on ContentItem, DynamoDBState, S3Storage

## Architecture Notes
- Each stage of the pipeline uses the same ContentItem dataclass, with different fields populated at each stage
- Storage classes (`DynamoDBState`, `S3Storage`) handle serialization/deserialization between ContentItem objects and database/object storage
- Each pipeline component should focus on its core responsibility, with minimal knowledge of other components' implementation details

1. **Refactor Item Loading Logic**: There's significant overlap in the logic at the beginning of `run_process_stage` and `run_summarize_stage` for determining which items to work on based on specific_id, overwrite_flag, and whether items were passed from a previous stage. This logic could be extracted into a reusable helper function (or potentially integrated into the `DynamoDBState` class) to reduce duplication and improve clarity. For example, a function like `get_items_for_stage(stage_name, specific_id, overwrite, previous_stage_items)` could encapsulate this.


2. **Introduce Data Classes or Pydantic Models**: The pipeline passes around lists of dictionaries (List[Dict]). While flexible, this makes it hard to know the expected structure of an 'item' and can lead to errors (e.g., typos in keys). Using Python's dataclasses or a library like Pydantic to define a clear Item model would improve type hinting, code readability, auto-completion, and overall robustness. You'd know exactly what fields (guid, title, link, s3_path, is_processed, etc.) an item should have at different stages.

3. **Centralize Configuration**: Several hardcoded values are scattered throughout the script, such as S3 key prefixes ("html/", "markdown/", "curated/", "processed/summaries/"), temporary file paths ("/tmp/last_processed_item.md"), and default parameters (most_recent=10 in main). Moving these into a dedicated configuration file (e.g., YAML, JSON) or consistently using environment variables (managed via .env) would make the application easier to configure, deploy, and maintain.

4. **Modularize Pipeline Stage Logic**: While the run_*_stage functions provide some separation, main.py still contains the core orchestration and significant implementation details for each stage. Consider moving the primary logic of each stage into the respective classes (RssFetcher, MarkdownProcessor, Summarizer, NewsletterCurator). main.py would then become much simpler, focusing on parsing arguments, setting up services, and calling methods on these classes to execute the pipeline steps. This improves separation of concerns and makes the individual components easier to test and modify.

5. **Enhance Error Handling and State Logging**: The current error handling primarily relies on logging warnings or errors. Consider adding more specific exception handling, especially around AWS interactions (S3 get_content/store_content, DynamoDB operations) using try...except blocks for botocore.exceptions.ClientError. This allows for more graceful failure modes or retry logic if needed. Additionally, logging state transitions (e.g., "Starting processing for item X", "Successfully stored markdown for item X", "Failed to summarize item Y") more explicitly can significantly aid debugging.


## Ensuring Reusability under different architectures:
**Shift Orchestration Logic into Core Classes**: The run_*_stage functions in main.py currently handle too much logic beyond just calling the relevant class. Move more of the "how" into the classes themselves.
- Example (`run_fetch_stage`): Instead of main.py checking for existing items and deciding whether to update or create metadata, the RssFetcher (perhaps renamed or having a dedicated method like fetch_and_update_state) should encapsulate fetching and interacting with DynamoDBState to store/update the fetched item metadata and S3Storage to store HTML. main.py would simply call fetcher.fetch_and_update_state(specific_url=args.rss_url) and get back a list of fetched item identifiers or - minimal metadata needed for the next stage.
- Example (`run_process_stage`): The MarkdownProcessor should have a method like process_item(item_metadata) or process_batch(items_metadata) that takes item metadata (including guid and html_path), uses S3Storage to get the HTML, performs processing, uses S3Storage again to store the markdown, and uses DynamoDBState to update the status (is_processed, s3_path, is_paywall, etc.). main.py's run_process_stage would focus only on getting the list of items to process (using the potential get_items_for_stage helper discussed previously) and then iterating, - calling processor.process_item(item) for each.
- `run_summarize_stage`:
- `run_curate_stage`:


**Strictly Define Class Responsibilities & Interactions**: Ensure each class has a laser - focus and interacts with others through well-defined interfaces (methods).
RssFetcher: Responsible only for fetching content from RSS feeds and returning structured data (like raw HTML, title, link, dates). It might interact with storage - to save the raw HTML, but shouldn't know about processing or summarization statuses.
MarkdownProcessor: Takes raw HTML (or a reference like an S3 path), converts it to Markdown, determines if it's suitable for summarization (is_paywall, length checks), and returns the Markdown content and flags. It interacts with storage to get HTML - and save Markdown.
Summarizer: Takes Markdown content (or a reference), generates summaries, and - returns them. Interacts with storage to get Markdown and save summaries.
NewsletterCurator: Queries the DynamoDBState for items meeting curation criteria (e.g., summarized recently), fetches necessary summaries using S3Storage, formats the newsletter, saves it using S3Storage, and potentially updates item state via - DynamoDBState.
DynamoDBState & S3Storage: Solely responsible for interacting with AWS DynamoDB and - S3 respectively. No business logic like parsing or summarization should exist here.
Use Data Classes/Models for Items: (Reiterating point #2 from the initial suggestions as it's crucial for separation). Define a Pydantic model or Python dataclass for ContentItem. Different stages might receive or return slightly different versions of this model (e.g., the fetcher output might not have markdown_path, the processor output adds it). This enforces clear data contracts - between stages/classes instead of passing ambiguous dictionaries.

**Make Stages Data-Driven via State Manager**: Reduce the direct passing of large lists of items between run_*_stage functions in main.py. Instead, make main.py primarily manage the flow and let each stage query the DynamoDBState manager for the items it - needs to work on based on status flags (is_fetched, is_processed, is_summarized).
- main.py calls fetcher.fetch_and_update_state().
- main.py calls processor.process_eligible_items(). Inside this method, the processor asks DynamoDBState for items where is_fetched=True and is_processed=False -(respecting overwrite and specific_id).
- main.py calls summarizer.summarize_eligible_items(). Inside, the summarizer asks DynamoDBState for items where is_processed=True and is_summarized=False (respecting - flags).
main.py calls curator.generate_newsletter(). Inside, the curator asks DynamoDBState - for relevant items.
This makes the stages more independent; the processor doesn't strictly need the - direct output list from the fetcher.

**Centralize Configuration Loading**: (Reiterating point #3 from initial suggestions). Ensure all configuration values (S3 paths, table names, default parameters like max_items or most_recent, temporary paths) are loaded from environment variables (dotenv) or a dedicated config file at the start of main.py and passed explicitly as arguments during the initialization of services (DynamoDBState, S3Storage) and core logic classes (RssFetcher, Summarizer, etc.). Avoid accessing os.getenv or hardcoded - strings deep within the logic classes.
