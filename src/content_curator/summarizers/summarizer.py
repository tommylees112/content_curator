from typing import Any, Dict, List, Optional

from langchain.chains.summarize import load_summarize_chain
from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
from loguru import logger


class Summarizer:
    """Handles content summarization using LangChain."""

    def __init__(self, model_name: str = "gpt-3.5-turbo", temperature: float = 0.0):
        """
        Initialize the summarizer with a language model.

        Args:
            model_name: The name of the LLM to use for summarization
            temperature: Temperature setting for the LLM (0.0 for most deterministic output)
        """
        self.logger = logger

        # Initialize the language model
        try:
            self.llm: BaseChatModel = ChatOpenAI(
                model_name=model_name, temperature=temperature
            )
            self.logger.info(f"Initialized summarizer with model: {model_name}")
        except Exception as e:
            self.logger.error(
                f"Failed to initialize language model: {e}", exc_info=True
            )
            raise

    def summarize_text(self, content: str, max_tokens: int = 500) -> Optional[str]:
        """
        Generate a summary of the provided content using LangChain.

        Args:
            content: The text content to summarize
            max_tokens: Maximum length of the summary in tokens

        Returns:
            A summary of the content or None if summarization fails
        """
        if not content or not isinstance(content, str):
            self.logger.warning(
                f"Invalid content provided for summarization: {type(content)}"
            )
            return None

        try:
            # Create a Document from the content
            doc = Document(page_content=content)

            # Load the summarization chain with the "stuff" method for shorter content
            chain = load_summarize_chain(
                self.llm,
                chain_type="stuff",  # "map_reduce" would be better for very long content
                verbose=True,
            )

            # Run the summarization
            summary = chain.invoke([doc])

            # Clean up the summary
            if isinstance(summary, str):
                return summary.strip()
            else:
                self.logger.warning(
                    f"Summarization returned non-string: {type(summary)}"
                )
                # Convert to string if possible
                return str(summary).strip()

        except Exception as e:
            self.logger.error(f"Failed to summarize content: {e}", exc_info=True)
            return None

    def batch_summarize(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Summarize a batch of content items.

        Args:
            items: List of content items with markdown_content to summarize

        Returns:
            List of items with summaries added
        """
        summarized_items = []

        for item in items:
            guid = item.get("guid", "unknown")
            content = item.get("markdown_content")

            if not content:
                self.logger.warning(f"No content to summarize for item {guid}")
                item["summary"] = None
                summarized_items.append(item)
                continue

            self.logger.info(f"Summarizing content for item {guid}")

            # Generate summary
            summary = self.summarize_text(content)

            # Add summary to the item
            item["summary"] = summary
            summarized_items.append(item)

        self.logger.info(f"Summarized {len(summarized_items)} items")
        return summarized_items
