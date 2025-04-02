# TODO
# Content Curator Improvements

## Completed
- [x] Added `ContentItem` dataclass in `src/content_curator/models.py` to represent content items throughout the pipeline
- [x] Updated `DynamoDBState` to use ContentItem with new `store_item`, `get_item`, and `update_item` methods
- [x] Updated `RSSFetcher` to return ContentItem objects
- [x] Updated `MarkdownProcessor` to work with ContentItem objects
- [x] Updated `Summarizer` to work with ContentItem objects
- [x] Updated `NewsletterCurator` to work with ContentItem objects
- [x] Updated `main.py` to use ContentItem throughout the pipeline stages
- [x] Updated scripts to use ContentItem objects (`update_guids.py`, `fix_summary_metadata.py`)
- [x] Updated examples to use ContentItem objects (`summarization_process.py`)
- [x] Added `get_items_needing_summarization` method to DynamoDBState to get items that need summarization
- [x] ensure that the fetcher stores the html that is collected in the s3 storage.create a new html_path field in the ContentItem dataclass and column in the dynamodb table. Also rename / update the s3_path to be md_path.
- [x] Clean up original dictionary-based methods after fully validating the ContentItem implementation
- [x] refactor the ContentItem and its uses throughout the pipeline to simply look for summary_path and short_summary_path instead of having the boolean flags: is_fetched, is_processed, is_summarized, is_distributed. Processes that use these  should just look at the paths. in all of the fetchers, processors, summarizers and curators. Remove references to the boolean flags. Do not give warnings or deprecations, just make the changes.
- [x] Only run the short summary stage by default. We can do the full summary in two ways 1) run when the user asks for it 2) run when that item is part of a newsletter / curated into the newsletter. Come up with designs and ways of doing this that maintain the separation of concerns between stages.
- [x] Consider refactoring the pipeline to make each stage more independent:
  - [x] Extract the `get_items_for_stage()` helper function to centralize item loading logic
  - [x] Move more stage logic from `main.py` to the respective classes
- [x] Make configuration more centralized, moving hardcoded values into a config file
- [x] rename RssFetcher to RSSFetcher
- [x] Create a distributor class that will take in a list of content items and distribute them to the appropriate channels. how to do this in an extensible way (slack, email, whatsapp, html, etc.).
- [x] deploy this and have it run daily on github actions
- [x] from the AWSURLDistributor create a view into the LONGER summaries of the curated content so the user can click through and see the full summaries.
- [x] Newsletter should have ability to create a full summary when it does not yet exist
```python
    def ensure_full_summaries(self, items: List[ContentItem]) -> List[ContentItem]:
        """Ensure all items have full summaries before including in newsletter."""
        summarizer = Summarizer(
            model_name="gemini-1.5-flash",
            s3_storage=self.s3_storage,
            state_manager=self.state_manager,
        )
        
        # Filter items that need full summaries
        items_needing_full = [
            item for item in items 
            if not item.summary_path or not self.s3_storage.check_content_exists_at_paths(
                guid=item.guid,
                path_formats=["processed/summaries/{guid}.md"],
                configured_path=item.summary_path
            )
        ]
        
        if items_needing_full:
            # Generate full summaries for these items
            summarizer.summarize_and_update_state(
                items_needing_full,
                overwrite_flag=False,
                summary_types=["standard"]
            )
        
        return items

    def curate_newsletter(self, items: List[ContentItem]) -> str:
        """Curate newsletter content, ensuring full summaries exist."""
        # First ensure all items have full summaries
        items = self.ensure_full_summaries(items)
        
        # Then format the newsletter with full summaries
        return self.format_recent_content(items, summary_type="standard")
```

## TODO
- [ ] Describe the images in the standard form summary (`Summarizer prompts`, `standard_prompt.txt`)
- [ ] think about how to include the most informative images in the newsletter (`Summarizer`)
- [ ] Links to the content like a table of contents at the start of the curated content (`Curator` and the `EmailDistributor`)
- [ ] Add randomly from corpus with some probability if the content has already been included 
- [ ] Assign probability weights to the different rss feeds / sources in the config file `config.py`  and then use these in the `Curator` to determine what gets selected for the newsletter.
- [ ] move the rss feeds that we are reading into the config file `config.py` 
- [ ] Have a link underneath each longer form summary which takes you to the longer form url created by the AWS s3 article url. Do we do this in the curator? or at the distribution stage? Does it ruin our separation of concerns if they're combined in each?
- [ ] codebase follows the dependency injection pattern partially by passing s3_storage and state_manager as parameters rather than importing them globally. Consider extending this pattern to configuration values as well for better maintainability as the application grows.
- [ ] Add comprehensive unit tests for the ContentItem implementation
- [ ] Add docstrings explaining the purpose and lifecycle of ContentItem fields
- [ ] Prepare for potential serverless architecture:
  - [ ] Create a `functions/` directory with handlers for each pipeline stage
  - [ ] Implement serverless function entry points for each stage
  - [ ] Set up infrastructure as code (e.g., AWS SAM or Serverless Framework config)
- [ ] Improve error handling with specific exception types and retry logic
- [ ] separation of concerns means that the differnet components do not rely on each other but only on ContentItem, DynamoDBState, S3Storage. Check that all of the components are only using ContentItem, DynamoDBState, S3Storage and utils interfaces and not each other / other components. 
- [ ] maintain a very strict set of data contracts between the components - define the contracts and enforce them. Where should these be defined? In the directory? in a subdirectory README or the original README? or somewhere else?.
- [ ] Can we use Astronomer and Airflow to run and orchestrate the pipeline? I'd be intereted to see if and how this works.

## Architecture Notes
- Each stage of the pipeline uses the same ContentItem dataclass, with different fields populated at each stage
- Storage classes (`DynamoDBState`, `S3Storage`) handle serialization/deserialization between ContentItem objects and database/object storage
- Each pipeline component should focus on its core responsibility, with minimal knowledge of other components' implementation details

1. **Refactor Item Loading Logic**: There's significant overlap in the logic at the beginning of `run_process_stage` and `run_summarize_stage` for determining which items to work on based on specific_id, overwrite_flag, and whether items were passed from a previous stage. This logic could be extracted into a reusable helper function (or potentially integrated into the `DynamoDBState` class) to reduce duplication and improve clarity. For example, a function like `get_items_for_stage(stage_name, specific_id, overwrite, previous_stage_items)` could encapsulate this.


2. **Introduce Data Classes or Pydantic Models**: The pipeline passes around lists of dictionaries (List[Dict]). While flexible, this makes it hard to know the expected structure of an 'item' and can lead to errors (e.g., typos in keys). Using Python's dataclasses or a library like Pydantic to define a clear Item model would improve type hinting, code readability, auto-completion, and overall robustness. You'd know exactly what fields (guid, title, link, s3_path, is_processed, etc.) an item should have at different stages.

3. **Centralize Configuration**: Several hardcoded values are scattered throughout the script, such as S3 key prefixes ("html/", "markdown/", "curated/", "processed/summaries/"), temporary file paths ("/tmp/last_processed_item.md"), and default parameters (most_recent=10 in main). Moving these into a dedicated configuration file (e.g., YAML, JSON) or consistently using environment variables (managed via .env) would make the application easier to configure, deploy, and maintain.

4. **Modularize Pipeline Stage Logic**: While the run_*_stage functions provide some separation, main.py still contains the core orchestration and significant implementation details for each stage. Consider moving the primary logic of each stage into the respective classes (RSSFetcher, MarkdownProcessor, Summarizer, NewsletterCurator). main.py would then become much simpler, focusing on parsing arguments, setting up services, and calling methods on these classes to execute the pipeline steps. This improves separation of concerns and makes the individual components easier to test and modify.

5. **Enhance Error Handling and State Logging**: The current error handling primarily relies on logging warnings or errors. Consider adding more specific exception handling, especially around AWS interactions (S3 get_content/store_content, DynamoDB operations) using try...except blocks for botocore.exceptions.ClientError. This allows for more graceful failure modes or retry logic if needed. Additionally, logging state transitions (e.g., "Starting processing for item X", "Successfully stored markdown for item X", "Failed to summarize item Y") more explicitly can significantly aid debugging.


## Ensuring Reusability under different architectures:
**Shift Orchestration Logic into Core Classes**: The run_*_stage functions in main.py currently handle too much logic beyond just calling the relevant class. Move more of the "how" into the classes themselves.
- Example (`run_fetch_stage`): Instead of main.py checking for existing items and deciding whether to update or create metadata, the RSSFetcher (perhaps renamed or having a dedicated method like fetch_and_update_state) should encapsulate fetching and interacting with DynamoDBState to store/update the fetched item metadata and S3Storage to store HTML. main.py would simply call fetcher.fetch_and_update_state(specific_url=args.rss_url) and get back a list of fetched item identifiers or - minimal metadata needed for the next stage.
- Example (`run_process_stage`): The MarkdownProcessor should have a method like process_item(item_metadata) or process_batch(items_metadata) that takes item metadata (including guid and html_path), uses S3Storage to get the HTML, performs processing, uses S3Storage again to store the markdown, and uses DynamoDBState to update the status (is_processed, s3_path, is_paywall, etc.). main.py's run_process_stage would focus only on getting the list of items to process (using the potential get_items_for_stage helper discussed previously) and then iterating, - calling processor.process_item(item) for each.
- `run_summarize_stage`:
- `run_curate_stage`:


**Strictly Define Class Responsibilities & Interactions**: Ensure each class has a laser - focus and interacts with others through well-defined interfaces (methods).
`RSSFetcher`: Responsible only for fetching content from RSS feeds and returning structured data (like raw HTML, title, link, dates). It might interact with storage - to save the raw HTML, but shouldn't know about processing or summarization statuses.
`MarkdownProcessor`: Takes raw HTML (or a reference like an S3 path), converts it to Markdown/ It interacts with storage to get HTML - and save Markdown.
`Summarizer`: Takes Markdown content (or a reference), determines if it's going to be summarized generates summaries, and - returns them. Interacts with storage to get Markdown and save summaries.
`NewsletterCurator`: Queries the DynamoDBState for items meeting curation criteria (e.g., summarized recently), fetches necessary summaries using S3Storage, formats the newsletter, saves it using S3Storage, and potentially updates item state via - DynamoDBState.
`DynamoDBState` & `S3Storage`: Solely responsible for interacting with AWS DynamoDB and - S3 respectively. No business logic like parsing or summarization should exist here.
Use Data Classes/Models for Items: (Reiterating point #2 from the initial suggestions as it's crucial for separation). Define a Pydantic model or Python dataclass for `ContentItem`. Different stages might receive or return slightly different versions of this model (e.g., the fetcher output might not have markdown_path, the processor output adds it). This enforces clear data contracts - between stages/classes instead of passing ambiguous dictionaries.

**Make Stages Data-Driven via State Manager**: Reduce the direct passing of large lists of items between run_*_stage functions in main.py. Instead, make main.py primarily manage the flow and let each stage query the DynamoDBState manager for the items it - needs to work on based on status flags (is_fetched, is_processed, is_summarized).
- main.py calls fetcher.fetch_and_update_state().
- main.py calls processor.process_eligible_items(). Inside this method, the processor asks DynamoDBState for items where is_fetched=True and is_processed=False -(respecting overwrite and specific_id).
- main.py calls summarizer.summarize_eligible_items(). Inside, the summarizer asks DynamoDBState for items where is_processed=True and is_summarized=False (respecting - flags).
main.py calls curator.generate_newsletter(). Inside, the curator asks DynamoDBState - for relevant items.
This makes the stages more independent; the processor doesn't strictly need the - direct output list from the fetcher.

**Centralize Configuration Loading**: (Reiterating point #3 from initial suggestions). Ensure all configuration values (S3 paths, table names, default parameters like max_items or most_recent, temporary paths) are loaded from environment variables (dotenv) or a dedicated config file at the start of main.py and passed explicitly as arguments during the initialization of services (DynamoDBState, S3Storage) and core logic classes (RSSFetcher, Summarizer, etc.). Avoid accessing os.getenv or hardcoded - strings deep within the logic classes.
