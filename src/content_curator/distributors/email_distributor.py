import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from loguru import logger

from src.content_curator.config import config
from src.content_curator.distributors.aws_url_distributor import AWSURLDistributor
from src.content_curator.distributors.html_converter import (
    HTMLConverter,
    convert_markdown_to_html,
)
from src.content_curator.storage.s3_storage import S3Storage


class EmailDistributor:
    """
    Distributes content by sending it as an HTML email to specified recipients.
    """

    def __init__(
        self,
        s3_storage: S3Storage,
        smtp_server: Optional[str] = None,
        smtp_port: Optional[int] = None,
        sender_email: Optional[str] = None,
        sender_password: Optional[str] = None,
        bucket_name: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        """
        Initializes the distributor with the S3 storage and configuration.

        Args:
            s3_storage: The S3 storage manager.
            smtp_server: SMTP server address. If None, will use the configured server.
            smtp_port: SMTP port. If None, will use the configured port.
            sender_email: Email to send from. If None, will use the configured email.
            sender_password: Password for sender email. If None, will use the configured password.
            bucket_name: The name of the S3 bucket. If None, will use the configured bucket.
            region_name: The AWS region where the bucket is located. If None,
                        will use the configured region.
        """
        self.s3_storage = s3_storage
        self.smtp_server = smtp_server or config.smtp_server
        self.smtp_port = smtp_port or config.smtp_port
        self.sender_email = sender_email or config.sender_email
        self.sender_password = sender_password or config.sender_password
        self.bucket_name = bucket_name or config.s3_bucket_name
        self.region_name = region_name or config.aws_region

        # Log credential status with masking
        masked_email = (
            self._mask_string(self.sender_email) if self.sender_email else "None"
        )
        has_password = "Yes" if self.sender_password else "No"
        masked_password = (
            self._mask_string(self.sender_password) if self.sender_password else "None"
        )

        logger.info(
            f"Email configuration loaded - Server: {self.smtp_server}:{self.smtp_port}"
        )
        logger.info(f"Sender email: {masked_email}")
        logger.info(
            f"Sender password provided: {has_password}, len: {len(masked_password)}"
        )

        if not self.bucket_name:
            raise ValueError("S3 bucket name must be provided.")
        if not self.smtp_server:
            raise ValueError("SMTP server must be provided.")
        if not self.sender_email:
            raise ValueError("Sender email must be provided.")
        if not self.sender_password:
            raise ValueError("Sender password must be provided.")

        # Initialize the HTML converter
        self.html_converter = HTMLConverter(s3_storage, bucket_name, region_name)

        # Initialize the AWS URL distributor
        self.url_distributor = AWSURLDistributor(s3_storage, bucket_name, region_name)

        logger.info(f"Initialized EmailDistributor for bucket: {self.bucket_name}")

    def _mask_string(self, value: str) -> str:
        """
        Mask a string to hide sensitive information.
        Shows the first and last characters, with asterisks in between.

        Args:
            value: The string to mask.

        Returns:
            The masked string.
        """
        if not value or len(value) <= 2:
            return "****"

        return value[0] + "*" * (len(value) - 2) + value[-1]

    def distribute(
        self,
        s3_key: str = "curated/latest.md",
        recipient_email: Optional[str] = None,
        subject_prefix: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> bool:
        """
        Retrieves markdown content from S3, converts it to HTML, and sends it via email.

        Args:
            s3_key: The key (path) of the object in the S3 bucket. Defaults to "curated/latest.md".
            recipient_email: Email to send to. If None, will use the configured default.
            subject_prefix: Prefix for the subject line. If None, will use the configured prefix.
            subject: Custom subject line. If None, will use the file name.

        Returns:
            True if the email was sent successfully, False otherwise.
        """
        try:
            # Get default recipient if not provided
            recipient_email = recipient_email or config.default_recipient
            if not recipient_email:
                logger.error("No recipient email provided and no default configured.")
                return False

            # Check if the object exists
            if not self.s3_storage.object_exists(s3_key):
                logger.warning(
                    f"Object s3://{self.bucket_name}/{s3_key} does not exist."
                )
                return False

            # Get the markdown content
            markdown_content = self.s3_storage.get_content(s3_key)
            if not markdown_content:
                logger.warning(
                    f"Could not retrieve content from s3://{self.bucket_name}/{s3_key}."
                )
                return False

            # Convert the markdown to HTML
            html_content = convert_markdown_to_html(markdown_content)

            # Generate browser view URL with HTML version instead of markdown
            browser_url = self.url_distributor.distribute_as_html(s3_key)
            if browser_url:
                browser_link = f'<div style="margin-bottom: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px;"><a href="{browser_url}">Follow link to view in browser</a></div>'
                # Insert the browser link at the beginning of the HTML body
                if "<body>" in html_content:
                    html_content = html_content.replace(
                        "<body>", f"<body>\n{browser_link}"
                    )
                else:
                    html_content = f"{browser_link}\n{html_content}"
                logger.info("Added HTML browser view link to email content")

            # Create email subject
            subject_prefix = subject_prefix or config.email_subject_prefix
            if not subject:
                # Extract file name from S3 key
                file_name = s3_key.split("/")[-1]
                subject = f"{subject_prefix}Content Update: {file_name}"
            elif subject_prefix:
                subject = f"{subject_prefix}{subject}"

            # Send the email
            masked_recipient = self._mask_string(recipient_email)
            logger.info(f"Attempting to send email to: {masked_recipient}")
            return self._send_email(recipient_email, subject, html_content)

        except Exception as e:
            logger.error(f"Failed to distribute content via email: {e}")
            return False

    def distribute_multiple(
        self,
        s3_keys: List[str],
        recipient_email: Optional[str] = None,
        subject_prefix: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> bool:
        """
        Combines multiple markdown files from S3, converts them to HTML, and sends via email.

        Args:
            s3_keys: List of S3 object keys to include in the email.
            recipient_email: Email to send to. If None, will use the configured default.
            subject_prefix: Prefix for the subject line. If None, will use the configured prefix.
            subject: Custom subject line. If None, will use a generic title.

        Returns:
            True if the email was sent successfully, False otherwise.
        """
        try:
            # Get default recipient if not provided
            recipient_email = recipient_email or config.default_recipient
            if not recipient_email:
                logger.error("No recipient email provided and no default configured.")
                return False

            # Create combined HTML content
            combined_html_content = ""
            for s3_key in s3_keys:
                # Check if the object exists
                if not self.s3_storage.object_exists(s3_key):
                    logger.warning(
                        f"Object s3://{self.bucket_name}/{s3_key} does not exist. Skipping."
                    )
                    continue

                # Get the markdown content
                markdown_content = self.s3_storage.get_content(s3_key)
                if not markdown_content:
                    logger.warning(
                        f"Could not retrieve content from s3://{self.bucket_name}/{s3_key}. Skipping."
                    )
                    continue

                # Convert the markdown to HTML and add to combined content
                html_content = convert_markdown_to_html(markdown_content)
                # Extract just the body content
                body_content = html_content.split("<body>")[1].split("</body>")[0]
                combined_html_content += (
                    f"<h2>{s3_key.split('/')[-1]}</h2>\n{body_content}\n<hr>\n"
                )

            if not combined_html_content:
                logger.error("No content was retrieved to send via email.")
                return False

            # Generate browser view URL for the first item (as a representative)
            browser_url = None
            if s3_keys:
                browser_url = self.url_distributor.distribute_as_html(s3_keys[0])

            browser_link = ""
            if browser_url:
                browser_link = f'<div style="margin-bottom: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px;"><a href="{browser_url}">Follow link to view in browser</a></div>'
                logger.info("Added HTML browser view link to email content")

            # Create full HTML document
            full_html = f"""<!DOCTYPE html>
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
    {browser_link}
    {combined_html_content}
</body>
</html>"""

            # Create email subject
            subject_prefix = subject_prefix or config.email_subject_prefix
            if not subject:
                subject = f"{subject_prefix}Content Curator Update"
            elif subject_prefix:
                subject = f"{subject_prefix}{subject}"

            # Send the email
            masked_recipient = self._mask_string(recipient_email)
            logger.info(
                f"Attempting to send multiple content email to: {masked_recipient}"
            )
            return self._send_email(recipient_email, subject, full_html)

        except Exception as e:
            logger.error(f"Failed to distribute multiple content files via email: {e}")
            return False

    def _send_email(
        self, recipient_email: str, subject: str, html_content: str
    ) -> bool:
        """
        Sends an HTML email to the specified recipient.

        Args:
            recipient_email: The email address to send to.
            subject: The subject line of the email.
            html_content: The HTML content of the email.

        Returns:
            True if the email was sent successfully, False otherwise.
        """
        try:
            # Create the email message
            message = MIMEMultipart()
            message["From"] = self.sender_email
            message["To"] = recipient_email
            message["Subject"] = subject

            # Attach the HTML content
            message.attach(MIMEText(html_content, "html"))

            # Connect to the SMTP server and send the email
            logger.debug(
                f"Connecting to SMTP server: {self.smtp_server}:{self.smtp_port}"
            )
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                logger.debug(
                    f"Attempting login for: {self._mask_string(self.sender_email)}"
                )
                server.login(self.sender_email, self.sender_password)
                logger.debug("SMTP login successful")
                server.send_message(message)

            logger.info(
                f"Successfully sent email to {self._mask_string(recipient_email)}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False


if __name__ == "__main__":
    # Initialize the S3 storage
    s3_storage = S3Storage(
        s3_bucket_name=config.s3_bucket_name,
        aws_region=config.aws_region,
    )

    # Initialize the distributor
    distributor = EmailDistributor(s3_storage=s3_storage)

    # Example usage
    success = distributor.distribute("curated/latest.md")
    if success:
        logger.info("Successfully sent email with latest content")
