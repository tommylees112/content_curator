import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from dotenv import load_dotenv
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from loguru import logger

# Define prompt types
SummaryType = Literal["standard", "brief"]
ModelName = Literal[
    "gemini-1.5-flash", "gemini-2.0-flash", "gpt-3.5-turbo", "gpt-4-turbo", "gpt-4o"
]

load_dotenv()


class Summarizer:
    """Handles content summarization using LangChain."""

    def __init__(
        self,
        model_name: ModelName = "gemini-1.5-flash",
        temperature: float = 0.0,
        max_output_tokens: Optional[int] = None,
    ):
        """
        Initialize the summarizer with a language model and load prompts from files.

        Args:
            model_name: The name of the LLM to use for summarization
            temperature: Temperature setting for the LLM (0.0 for most deterministic output)
            max_output_tokens: Maximum number of tokens to allow for the model's output
        """
        self.logger = logger
        self.model_name = model_name
        self.prompt_templates: Dict[str, str] = {}

        # Load prompts from files
        summarizer_dir = Path(__file__).parent
        prompt_files = {
            "standard": summarizer_dir / "standard_summary.txt",
            "brief": summarizer_dir / "brief_summary.txt",
        }

        for summary_type, path in prompt_files.items():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self.prompt_templates[summary_type] = f.read()
                self.logger.info(
                    f"Successfully loaded '{summary_type}' prompt from {path}"
                )
            except FileNotFoundError:
                self.logger.error(f"Prompt file not found: {path}")
            except Exception as e:
                self.logger.error(
                    f"Failed to load '{summary_type}' prompt from {path}: {e}"
                )

        if not self.prompt_templates:
            raise ValueError(
                "No prompt templates could be loaded. Please ensure prompt files exist."
            )

        # Initialize the language model
        try:
            self.llm: BaseChatModel

            if model_name == "gemini-1.5-flash":
                # Check for Google API key
                google_api_key = os.environ.get("GOOGLE_API_KEY")
                if not google_api_key:
                    self.logger.warning(
                        "GOOGLE_API_KEY not found in environment variables. "
                        "Falling back to OpenAI (gpt-4-turbo)."
                    )
                    # Fall back to OpenAI if Google API key is not available
                    model_name = "gpt-4-turbo"
                else:
                    # Initialize Gemini model (Gemini 1.5 Flash)
                    model_kwargs = {}
                    if max_output_tokens is not None:
                        model_kwargs["max_output_tokens"] = max_output_tokens

                    self.llm = ChatGoogleGenerativeAI(
                        model="gemini-1.5-flash",
                        temperature=temperature,
                        google_api_key=google_api_key,
                        **model_kwargs,
                    )
                    self.logger.info("Successfully initialized Gemini 1.5 Flash model")

            # Initialize OpenAI model if needed
            if model_name != "gemini-1.5-flash":
                model_kwargs = {}
                if max_output_tokens is not None:
                    model_kwargs["max_tokens"] = max_output_tokens

                self.llm = ChatOpenAI(
                    model_name=model_name, temperature=temperature, **model_kwargs
                )
                self.logger.info(f"Using OpenAI model: {model_name}")

            self.logger.info(
                f"Initialized summarizer with model: {model_name}, "
                f"Max Output Tokens: {max_output_tokens or 'Default'}"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to initialize language model: {e}", exc_info=True
            )
            raise

    def summarize_text(
        self, content: str, summary_type: SummaryType = "standard"
    ) -> Optional[str]:
        """
        Generate a summary of the provided content.

        Args:
            content: The text content to summarize
            summary_type: The type of summary to generate ("standard" or "brief")

        Returns:
            A summary of the content or None if summarization fails
        """
        if not content or not isinstance(content, str) or not content.strip():
            self.logger.warning(
                f"Invalid content provided for summarization: {type(content)}"
            )
            return None

        # Get prompt for the specified summary type
        prompt = self.prompt_templates.get(summary_type)
        if not prompt:
            self.logger.error(
                f"Invalid summary type specified: '{summary_type}'. Available: {list(self.prompt_templates.keys())}"
            )
            return None

        try:
            # Create messages with system prompt and content
            messages = [SystemMessage(content=prompt), HumanMessage(content=content)]

            # Get response from the LLM
            response = self.llm.invoke(messages)

            # Extract the summary text
            summary = response.content.strip()

            if summary:
                self.logger.info(f"Successfully generated '{summary_type}' summary")
                return summary
            else:
                self.logger.warning(
                    f"'{summary_type}' summarization did not produce expected output"
                )
                return None

        except Exception as e:
            self.logger.exception(f"Failed to generate '{summary_type}' summary: {e}")
            return None

    def batch_summarize(
        self, items: List[Dict[str, Any]], summary_type: SummaryType = "standard"
    ) -> List[Dict[str, Any]]:
        """
        Generate summaries for a batch of items using the specified summary type.

        Args:
            items: List of content items with markdown_content
            summary_type: The type of summary to generate ("standard" or "brief")

        Returns:
            List of items with added 'summary' if standard or 'short_summary' if brief
        """
        results = []
        summary_field = "summary" if summary_type == "standard" else "short_summary"

        self.logger.info(
            f"Batch summarizing {len(items)} items with '{summary_type}' summary type"
        )

        for item in items:
            content = item.get("markdown_content", "")
            if not content:
                self.logger.warning(
                    f"Skipping item '{item.get('title', 'N/A')}' ({item.get('guid', 'N/A')}) due to missing markdown content."
                )
                # Don't modify the item if no content
                results.append(item.copy())
                continue

            # Generate the requested summary type
            summary = self.summarize_text(content, summary_type=summary_type)

            # Add to results, preserving existing data
            item_with_summary = item.copy()
            item_with_summary[summary_field] = summary
            results.append(item_with_summary)

            if summary:
                self.logger.info(
                    f"Generated '{summary_type}' summary for '{item.get('title', 'N/A')}' ({item.get('guid', 'N/A')})"
                )

        self.logger.info(f"Generated {summary_type} summaries for {len(results)} items")
        return results

    def batch_summarize_all(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate both standard and brief summaries for a batch of items.

        This is a convenience method that calls batch_summarize twice,
        once for each summary type.

        Args:
            items: List of content items with markdown_content

        Returns:
            List of items with added 'summary' and 'short_summary'
        """
        # First, generate standard summaries
        items_with_standard = self.batch_summarize(items, summary_type="standard")

        # Then, generate brief summaries and merge them
        items_with_both = self.batch_summarize(
            items_with_standard, summary_type="brief"
        )

        self.logger.info(
            f"Generated both summary types for {len(items_with_both)} items"
        )
        return items_with_both


if __name__ == "__main__":
    summarizer = Summarizer(model_name="gemini-1.5-flash")
    test_file = Path(__file__).parents[3] / "data/test.md"
    with open(test_file, "r", encoding="utf-8") as f:
        content = f.read()

    print("Standard summary:")
    print(summarizer.summarize_text(content, summary_type="standard"))
    print("\nBrief summary:")
    print(summarizer.summarize_text(content, summary_type="brief"))
