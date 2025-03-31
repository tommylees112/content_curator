import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain.chains.summarize import load_summarize_chain
from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from loguru import logger

# Dictionary of known context windows
MODEL_CONTEXT_WINDOWS = {
    "gpt-3.5-turbo": 16385,
    "gpt-4": 8192,
    "gpt-4-32k": 32768,
    "gpt-4-turbo": 128000,
    "gpt-4o": 128000,
}

# Define prompt names
STANDARD_PROMPT_NAME = "standard"
BRIEF_PROMPT_NAME = "brief"


class Summarizer:
    """Handles content summarization using LangChain."""

    def __init__(
        self,
        model_name: str = "gpt-3.5-turbo",
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
        self.max_output_tokens = max_output_tokens
        self.prompt_templates: Dict[str, str] = {}

        # Load prompts from files
        summarizer_dir = Path(__file__).parent
        prompt_files = {
            STANDARD_PROMPT_NAME: summarizer_dir / "standard_summary.txt",
            BRIEF_PROMPT_NAME: summarizer_dir / "brief_summary.txt",
        }

        for name, path in prompt_files.items():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self.prompt_templates[name] = f.read()
                self.logger.info(f"Successfully loaded prompt '{name}' from {path}")
            except FileNotFoundError:
                self.logger.error(f"Prompt file not found: {path}")
            except Exception as e:
                self.logger.error(f"Failed to load prompt '{name}' from {path}: {e}")

        if not self.prompt_templates:
            raise ValueError(
                "No prompt templates could be loaded. Please ensure prompt files exist."
            )

        # Initialize the language model
        try:
            model_kwargs = {}
            if self.max_output_tokens is not None:
                model_kwargs["max_tokens"] = self.max_output_tokens

            self.llm: BaseChatModel = ChatOpenAI(
                model_name=self.model_name, temperature=temperature, **model_kwargs
            )
            self.logger.info(
                f"Initialized summarizer with model: {self.model_name}, Max Output Tokens: {self.max_output_tokens or 'Default'}"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to initialize language model: {e}", exc_info=True
            )
            raise

    def _get_model_context_window(self) -> Optional[int]:
        """Gets the context window size for the current model."""
        return MODEL_CONTEXT_WINDOWS.get(self.model_name)

    def summarize_text(
        self, content: str, prompt_name: str = STANDARD_PROMPT_NAME
    ) -> Optional[str]:
        """
        Generate a summary of the provided content using a specified prompt template.

        Args:
            content: The text content to summarize
            prompt_name: The name of the prompt template to use (e.g., "standard", "brief")

        Returns:
            A summary of the content or None if summarization fails or input is too long
        """
        if not content or not isinstance(content, str):
            self.logger.warning(
                f"Invalid content provided for summarization: {type(content)}"
            )
            return None

        # Get prompt template
        prompt_template_str = self.prompt_templates.get(prompt_name)
        if not prompt_template_str:
            self.logger.error(
                f"Invalid prompt name specified: '{prompt_name}'. Available: {list(self.prompt_templates.keys())}"
            )
            return None

        # Get context window
        context_window = self._get_model_context_window()
        if context_window:
            try:
                # Create the prompt template instance from the loaded string
                PROMPT = ChatPromptTemplate.from_template(prompt_template_str)

                # Format prompt and count tokens
                prompt_value = PROMPT.format_prompt(text=content)
                messages = prompt_value.to_messages()
                input_tokens = self.llm.get_num_tokens_from_messages(messages)
                output_buffer = self.max_output_tokens or 500

                # Compare and log/warn
                if input_tokens + output_buffer >= context_window:
                    self.logger.warning(
                        f"Input content (approx. {input_tokens} tokens) for prompt '{prompt_name}' + output buffer ({output_buffer} tokens) "
                        f"may exceed model's context window ({context_window} tokens) for model '{self.model_name}'. "
                        f"Skipping summarization."
                    )
                    return None
                else:
                    self.logger.debug(
                        f"Input token count ({input_tokens}) + buffer ({output_buffer}) within context window ({context_window}) for prompt '{prompt_name}'."
                    )

            except Exception as e:
                self.logger.error(
                    f"Could not estimate token count for prompt '{prompt_name}': {e}",
                    exc_info=True,
                )
                warnings.warn(
                    f"Could not estimate token count for model {self.model_name}. Proceeding without check."
                )

        else:
            self.logger.warning(
                f"Context window size not defined for model '{self.model_name}'. Cannot check input length."
            )
            warnings.warn(f"Context window size unknown for model {self.model_name}.")

        try:
            # (Re)Create the prompt template instance
            PROMPT = ChatPromptTemplate.from_template(prompt_template_str)

            # Load the summarization chain
            chain = load_summarize_chain(
                self.llm,
                chain_type="stuff",
                prompt=PROMPT,
                verbose=False,
            )

            # Create a Document
            doc = Document(page_content=content)

            # Run the summarization
            summary_output = chain.invoke([doc])

            # Extract the text
            summary = (
                summary_output.get("output_text", "")
                if isinstance(summary_output, dict)
                else str(summary_output)
            )

            if summary:
                self.logger.info(
                    f"Successfully generated summary using prompt: '{prompt_name}'"
                )
                return summary.strip()
            else:
                self.logger.warning(
                    f"Summarization using prompt '{prompt_name}' did not produce expected output: {summary_output}"
                )
                return None

        except Exception as e:
            self.logger.error(
                f"Failed to summarize content using prompt '{prompt_name}': {e}",
                exc_info=True,
            )
            return None

    def batch_summarize(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate standard and short summaries for a batch of items using specific prompts.

        Args:
            items: List of content items with markdown_content

        Returns:
            List of items with added 'summary' and 'short_summary'
        """
        results = []

        for item in items:
            content = item.get("markdown_content", "")
            if not content:
                self.logger.warning(
                    f"Skipping item {item.get('guid', 'N/A')} due to missing markdown content."
                )
                summary = None
                short_summary = None
            else:
                # Generate standard summary using "standard" prompt
                summary = self.summarize_text(content, prompt_name=STANDARD_PROMPT_NAME)
                # Generate short summary using "brief" prompt (via generate_short_summary)
                short_summary = self.generate_short_summary(content)

            # Add to results
            item_with_summary = item.copy()
            item_with_summary["summary"] = summary
            item_with_summary["short_summary"] = short_summary
            results.append(item_with_summary)

        self.logger.info(f"Attempted summarization for {len(results)} items")
        return results

    def generate_short_summary(self, content: str) -> Optional[str]:
        """
        Generate a shorter summary using the 'brief' prompt template.

        Args:
            content: The markdown content to summarize

        Returns:
            A short summary or None if summarization fails
        """
        # Call summarize_text specifically with the BRIEF_PROMPT_NAME
        return self.summarize_text(content, prompt_name=BRIEF_PROMPT_NAME)
