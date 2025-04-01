from typing import Optional

import markdown
from loguru import logger

from src.content_curator.config import config
from src.content_curator.storage.s3_storage import S3Storage


def convert_markdown_to_html(markdown_content: str) -> str:
    """
    Converts Markdown content to HTML with proper styling and link handling.

    Args:
        markdown_content: The Markdown content to convert.

    Returns:
        The HTML content with proper styling and clickable links.
    """
    # Convert Markdown to HTML with link detection
    html_content = markdown.markdown(
        markdown_content,
        extensions=[
            "markdown.extensions.extra",
            "markdown.extensions.codehilite",
            "markdown.extensions.toc",
            "markdown.extensions.nl2br",  # Convert newlines to <br>
        ],
        output_format="html5",
    )

    # Create HTML document with proper styling for links
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Content Curator</title>
    <style>
        body {{ font-family: sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }}
        a {{ color: #0366d6; text-decoration: underline; cursor: pointer; }}
        a:hover {{ color: #044289; text-decoration: underline; }}
        pre {{ background-color: #f6f8fa; padding: 16px; overflow: auto; }}
        blockquote {{ border-left: 4px solid #dfe2e5; padding: 0 1em; color: #6a737d; margin: 0; }}
    </style>
</head>
<body>
    {html_content}
    <script>
        // Ensure all URLs are clickable
        document.addEventListener('DOMContentLoaded', function() {{
            // Find all text nodes
            const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
            const nodes = [];
            let node;
            while (node = walk.nextNode()) {{
                nodes.push(node);
            }}
            
            // URL regex pattern - using a string constructor to avoid escape sequence issues
            const urlPattern = new RegExp("https?://[^\\s<>\"']+", "g");
            
            // Convert plain URL text to clickable links
            nodes.forEach(textNode => {{
                const parent = textNode.parentNode;
                if (parent.nodeName !== 'A' && parent.nodeName !== 'SCRIPT' && parent.nodeName !== 'STYLE') {{
                    const content = textNode.textContent;
                    const matches = content.match(urlPattern);
                    
                    if (matches) {{
                        let lastIndex = 0;
                        const fragment = document.createDocumentFragment();
                        
                        matches.forEach(url => {{
                            const urlIndex = content.indexOf(url, lastIndex);
                            
                            // Add text before URL
                            if (urlIndex > lastIndex) {{
                                fragment.appendChild(document.createTextNode(content.substring(lastIndex, urlIndex)));
                            }}
                            
                            // Add the URL as a link
                            const link = document.createElement('a');
                            link.href = url;
                            link.textContent = url;
                            link.target = '_blank';
                            fragment.appendChild(link);
                            
                            lastIndex = urlIndex + url.length;
                        }});
                        
                        // Add remaining text
                        if (lastIndex < content.length) {{
                            fragment.appendChild(document.createTextNode(content.substring(lastIndex)));
                        }}
                        
                        parent.replaceChild(fragment, textNode);
                    }}
                }}
            }});
        }});
    </script>
</body>
</html>"""

    return html


class HTMLConverter:
    """
    Converts Markdown content from S3 to HTML and stores it back in S3.
    """

    def __init__(
        self,
        s3_storage: S3Storage,
        bucket_name: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        """
        Initializes the HTML converter with the S3 storage and configuration.

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

        logger.info(f"Initialized HTMLConverter for bucket: {self.bucket_name}")

    def convert(
        self, markdown_key: str, html_key: Optional[str] = None
    ) -> Optional[str]:
        """
        Converts Markdown content from S3 to HTML and stores it back in S3.

        Args:
            markdown_key: The S3 key (path) of the Markdown content.
            html_key: The S3 key (path) to store the HTML content. If None,
                    will use the same path as markdown_key but with .html extension.

        Returns:
            The S3 key where the HTML was stored, or None if conversion failed.
        """
        try:
            # Check if the Markdown object exists
            if not self.s3_storage.object_exists(markdown_key):
                logger.warning(
                    f"Markdown object s3://{self.bucket_name}/{markdown_key} does not exist."
                )
                return None

            # Get the Markdown content
            markdown_content = self.s3_storage.get_content(markdown_key)
            if not markdown_content:
                logger.warning(
                    f"Could not retrieve content from s3://{self.bucket_name}/{markdown_key}."
                )
                return None

            # Convert Markdown to HTML
            html_content = convert_markdown_to_html(markdown_content)

            # Get the HTML key if not provided
            if not html_key:
                html_key = self.get_html_key_from_markdown_key(markdown_key)

            # Store the HTML content in S3
            success = self.s3_storage.store_content(
                html_key, html_content, content_type="text/html"
            )
            if not success:
                logger.error(f"Failed to store HTML content at {html_key}")
                return None

            logger.info(f"Stored HTML version at s3://{self.bucket_name}/{html_key}")
            return html_key

        except Exception as e:
            logger.error(f"Error converting markdown to HTML: {e}")
            return None

    def get_html_key_from_markdown_key(self, markdown_key: str) -> str:
        """
        Converts a Markdown file key to an HTML file key.

        Args:
            markdown_key: The S3 key for the Markdown file.

        Returns:
            The S3 key for the HTML file.
        """
        if markdown_key.endswith(".md"):
            return markdown_key[:-3] + ".html"
        else:
            return markdown_key + ".html"
