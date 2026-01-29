"""A module to send messages to Slack channels."""

import logging
import os
from functools import cache
from typing import Optional

from google.cloud.logging_v2._helpers import (
    retrieve_metadata_server,  # pyright: ignore[reportPrivateImportUsage]
)
from google.cloud.logging_v2.handlers._monitored_resources import (
    _PROJECT_NAME,  # pyright: ignore[reportPrivateImportUsage]
)
from pydantic import BaseModel, SecretStr
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from cleansweep import __app_name__, __version__
from cleansweep.settings.base import settings
from cleansweep.utils.slack.model import (
    Context,
    Markdown,
    MessageBlocks,
    RichText,
    RichTextPreformatted,
    RichTextSection,
    Section,
    SectionWithText,
    Text,
)

logger = logging.getLogger(__name__)

os.environ["SKIP_SLACK_SDK_WARNING"] = "true"

PROJECT_ID = retrieve_metadata_server(_PROJECT_NAME)
if PROJECT_ID is None:
    PROJECT_ID = "<unknown>"

ERROR_MAP = {
    "as_user_not_supported": "The as_user parameter does not function with workspace apps.",
    "channel_not_found": "Value passed for channel was invalid.",
    "duplicate_channel_not_found": "Channel associated with client_msg_id was invalid.",
    "duplicate_message_not_found": "No duplicate message exists associated with client_msg_id.",
    "ekm_access_denied": "Administrators have suspended the ability to post a message.",
    "invalid_blocks": "Blocks submitted with this message are not valid",
    "invalid_blocks_format": (
        "The blocks is not a valid JSON object or doesn't match the Block Kit syntax."
    ),
    "invalid_metadata_format": "Invalid metadata format provided",
    "invalid_metadata_schema": "Invalid metadata schema provided",
    "is_archived": "Channel has been archived.",
    "message_limit_exceeded": (
        "Members on this team are sending too many messages. For more details, see "
        "https://slack.com/help/articles/115002422943-Usage-limits-for-free-workspaces"
    ),
    "messages_tab_disabled": "Messages tab for the app is disabled.",
    "metadata_must_be_sent_from_app": (
        "Message metadata can only be posted or updated using an app-level token"
    ),
    "metadata_too_large": "Metadata exceeds size limit",
    "msg_blocks_too_long": "Blocks submitted with this message are too long.",
    "msg_too_long": "Message text is too long",
    "no_text": "No message text provided",
    "not_in_channel": "Cannot post user messages to a channel they are not in.",
    "rate_limited": (
        "Application has posted too many messages, read the Rate Limit documentation for more "
        "information"
    ),
    "restricted_action": "A workspace preference prevents the authenticated user from posting.",
    "restricted_action_non_threadable_channel": (
        "Cannot post thread replies into a non_threadable channel."
    ),
    "restricted_action_read_only_channel": "Cannot post any message into a read-only channel.",
    "restricted_action_thread_locked": (
        "Cannot post replies to a thread that has been locked by admins."
    ),
    "restricted_action_thread_only_channel": (
        "Cannot post top-level messages into a thread-only channel."
    ),
    "slack_connect_canvas_sharing_blocked": (
        "Admin has disabled Canvas File sharing in all Slack Connect communications"
    ),
    "slack_connect_file_link_sharing_blocked": (
        "Admin has disabled Slack File sharing in all Slack Connect communications"
    ),
    "slack_connect_lists_sharing_blocked": (
        "Admin has disabled Lists sharing in all Slack Connect communications"
    ),
    "team_access_not_granted": (
        "The token used is not granted the specific workspace access required to complete this "
        "request."
    ),
    "team_not_found": (
        "This error occurs if, when using an org-wide token, the channel_name is passed instead "
        "of the channel_id."
    ),
    "too_many_attachments": (
        "Too many attachments were provided with this message. A maximum of 100 attachments are "
        "allowed on a message."
    ),
    "too_many_contact_cards": (
        "Too many contact_cards were provided with this message. A maximum of 10 contact cards "
        "are allowed on a message."
    ),
    "cannot_reply_to_message": "This message type cannot have thread replies.",
    "missing_file_data": "Attempted to share a file but some required data was missing.",
    "attachment_payload_limit_exceeded": "Attachment payload size is too long.",
    "access_denied": "Access to a resource specified in the request is denied.",
    "account_inactive": (
        "Authentication token is for a deleted user or workspace when using a bot token."
    ),
    "deprecated_endpoint": "The endpoint has been deprecated.",
    "enterprise_is_restricted": "The method cannot be called from an Enterprise.",
    "invalid_auth": (
        "Some aspect of authentication cannot be validated. Either the provided token is invalid "
        "or the request originates from an IP address disallowed from making the request."
    ),
    "method_deprecated": "The method has been deprecated.",
    "missing_scope": (
        "The token used is not granted the specific scope permissions required to complete this "
        "request."
    ),
    "not_allowed_token_type": "The token type used in this request is not allowed.",
    "not_authed": "No authentication token provided.",
    "no_permission": (
        "The workspace token used in this request does not have the permissions necessary to "
        "complete the request. Make sure your app is a member of the conversation it's attempting "
        "to post a message to."
    ),
    "org_login_required": (
        "The workspace is undergoing an enterprise migration and will not be available until "
        "migration is complete."
    ),
    "token_expired": "Authentication token has expired",
    "token_revoked": (
        "Authentication token is for a deleted user or workspace or the app has been removed when "
        "using a user token."
    ),
    "two_factor_setup_required": "Two factor setup is required.",
    "accesslimited": "Access to this method is limited on the current network",
    "fatal_error": (
        "The server could not complete your operation(s) without encountering a catastrophic "
        "error. It's possible some aspect of the operation succeeded before the error was raised."
    ),
    "internal_error": (
        "The server could not complete your operation(s) without encountering an error, likely "
        "due to a transient issue on our end. It's possible some aspect of the operation "
        "succeeded before the error was raised."
    ),
    "invalid_arg_name": (
        "The method was passed an argument whose name falls outside the bounds of accepted or "
        "expected values. This includes very long names and names with non-alphanumeric "
        "characters other than _. If you get this error, it is typically an indication that you "
        "have made a very malformed API call."
    ),
    "invalid_arguments": (
        "The method was either called with invalid arguments or some detail about the arguments "
        "passed is invalid, which is more likely when using complex arguments like blocks or "
        "attachments."
    ),
    "invalid_array_arg": (
        "The method was passed an array as an argument. Please only input "
        "valid strings."
    ),
    "invalid_charset": (
        "The method was called via a POST request, but the charset specified in the Content-Type "
        "header was invalid. Valid charset names are: utf-8 iso-8859-1."
    ),
    "invalid_form_data": (
        "The method was called via a POST request with Content-Type "
        "application/x-www-form-urlencoded or multipart/form-data, but the form data was either "
        "missing or syntactically invalid."
    ),
    "invalid_post_type": (
        "The method was called via a POST request, but the specified Content-Type was invalid. "
        "Valid types are: application/json application/x-www-form-urlencoded multipart/form-data "
        "text/plain."
    ),
    "missing_post_type": (
        "The method was called via a POST request and included a data payload, but the request "
        "did not include a Content-Type header."
    ),
    "ratelimited": (
        "The request has been ratelimited. Refer to the Retry-After header for when to retry the "
        "request."
    ),
    "request_timeout": (
        "The method was called via a POST request, but the POST data was either missing or "
        "truncated."
    ),
    "service_unavailable": "The service is temporarily unavailable",
    "team_added_to_org": (
        "The workspace associated with your request is currently undergoing migration to an "
        "Enterprise Organization. Web API and other platform operations will be intermittently "
        "unavailable until the transition is complete."
    ),
}


@cache
def get_client(token: SecretStr | None = None) -> WebClient | None:
    """Return an instance of the Slack WebClient.

    Args:
        token (SecretStr | None): The Slack API token. Defaults to None.

    Returns:
        WebClient | None: An instance of the Slack WebClient if a token is provided, otherwise None.

    """
    if token is None:
        return None

    return WebClient(token=token.get_secret_value())


@cache
def get_context() -> Context:
    """Return the context block for a message."""
    return Context(
        elements=[
            Markdown(
                text=f"{__app_name__} v{__version__} | {PROJECT_ID} | {settings.name}"
            )
        ]
    )


class Message(BaseModel):
    """A class to represent a Slack message."""

    channel: str
    text: Optional[str] = None
    blocks: Optional[MessageBlocks] = None

    def send(self):
        """Send the message to the specified Slack channel."""
        app = get_client(settings.slack_bot_token)
        if app is None:
            logger.warning("Bot token not provided, unable to connect to Slack.")
            return

        try:
            _ = app.chat_postMessage(
                channel=self.channel,
                text=self.text,
                blocks=self.blocks.serialize_blocks if self.blocks else None,
            )
        except SlackApiError as e:
            error = ERROR_MAP.get(e.response["error"], e.response["error"])
            logger.error("Error sending message to Slack: %s", error)
            return


def send_error_message(channel: str, error: BaseException):
    """Send an error message to the specified Slack channel.

    Args:
        channel (str): The Slack channel to send the message to.
        error (BaseException): The error that occurred.

    """
    header = SectionWithText(
        text=Markdown(text="Oops! Something went wrong. :thumbsdown:")
    )
    body = RichText(
        elements=[
            RichTextSection(
                elements=[
                    Text(
                        text=(
                            f"An error has occurred during '{settings.app}' processing for "
                            f"{settings.name}."
                        )
                    )
                ]
            ),
            RichTextPreformatted(
                elements=[Text(text=f"{error.__class__.__name__}: {error}")]
            ),
        ]
    )

    if isinstance(error, ExceptionGroup):
        body.elements.append(
            RichTextSection(
                elements=[
                    Text(
                        text=(
                            "The following sub-exceptions occurred during the processing:"
                        )
                    )
                ]
            )
        )

        for i, exception in enumerate(error.exceptions):
            body.elements.append(
                RichTextPreformatted(
                    elements=[
                        Text(
                            text=f"Sub-exception {i + 1} - {exception.__class__.__name__}: {exception}"
                        )
                    ]
                )
            )

    Message(
        channel=channel, blocks=MessageBlocks(blocks=[header, body, get_context()])
    ).send()


def send_notification(channel: str, *lines: str):
    """Send a notification message to the specified Slack channel.

    Args:
        channel (str): The Slack channel to send the message to.
        *lines (str): The lines to include in the message. The lines can be Markdown formatted.

    """
    header = SectionWithText(text=Markdown(text="Hello there! :wave:"))
    body = [Section(fields=[Markdown(text=line)]) for line in lines]

    Message(
        channel=channel, blocks=MessageBlocks(blocks=[header, *body, get_context()])
    ).send()
