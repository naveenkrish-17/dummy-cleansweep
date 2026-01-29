"""Test the Slack message module."""

# pylint: disable=W0621
from unittest.mock import MagicMock, patch

import pytest
from slack_sdk.errors import SlackApiError

from cleansweep.utils.slack.message import (
    Message,
    MessageBlocks,
    send_error_message,
    send_notification,
)


@pytest.fixture
def mock_slack_client():
    """Mock the Slack client."""
    with patch("cleansweep.utils.slack.message.get_client") as mock:
        yield mock


@pytest.fixture
def mock_logger():
    """Mock the logger."""
    with patch("cleansweep.utils.slack.message.logger") as mock:
        yield mock


@pytest.fixture
def mock_blocks() -> MessageBlocks:
    """Mock message blocks."""
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "Hello there! :wave:"},
            },
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {"type": "text", "text": "I am the CleanSweep App."}
                        ],
                    },
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": "I will post notifications for pipelines, sharing errors and other important updates.",
                            }
                        ],
                    },
                ],
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": "CleanSweep v1.2.0-rc.1 | grp-cec-kosmo-dev | lighthouse",
                        "emoji": True,
                    }
                ],
            },
        ]
    }
    return MessageBlocks(**payload)


@pytest.fixture
def mock_settings():
    """Mock the settings object."""
    with patch("cleansweep.utils.slack.message.settings", autospec=True) as mock:
        mock.app = "TestApp"
        mock.name = "TestName"
        yield mock


@pytest.fixture
def slack_channel():
    """Return a sample Slack channel."""
    return "test-channel"


@pytest.fixture
def sample_error():
    """Return a sample error."""
    return ValueError("Sample error message")


@pytest.fixture
def mock_message_class():
    """Mock the Message class."""
    with patch("cleansweep.utils.slack.message.Message") as mock:
        yield mock


class TestMessage:
    """Test the Message class."""

    def test_message_initialization(
        self,
    ):
        """Test message initialization with only the channel argument."""
        message = Message(channel="test_channel")
        assert message.channel == "test_channel"
        assert message.text is None
        assert message.blocks is None

    def test_message_initialization_with_all_args(self, mock_blocks):
        """Test message initialization with all arguments."""
        message = Message(
            channel="test_channel", text="Hello, world!", blocks=mock_blocks
        )
        assert message.channel == "test_channel"
        assert message.text == "Hello, world!"
        assert message.blocks == mock_blocks

    def test_send_success(self, mock_slack_client, mock_blocks):
        """Test sending a message to Slack."""
        mock_slack_client.return_value.chat_postMessage = MagicMock()
        message = Message(
            channel="test_channel", text="Hello, world!", blocks=mock_blocks
        )
        message.send()
        mock_slack_client.return_value.chat_postMessage.assert_called_once_with(
            channel="test_channel",
            text="Hello, world!",
            blocks=mock_blocks.serialize_blocks,
        )

    def test_send_without_bot_token(self, mock_slack_client, mock_logger):
        """Test sending a message without providing a bot token."""
        mock_slack_client.return_value = None
        message = Message(channel="test_channel")
        message.send()
        mock_logger.warning.assert_called_once_with(
            "Bot token not provided, unable to connect to Slack."
        )

    def test_send_slack_api_error(self, mock_slack_client, mock_logger):
        """Test sending a message to Slack with an API error."""
        mock_slack_client.return_value.chat_postMessage.side_effect = SlackApiError(
            message="error", response={"error": "some_error"}
        )
        message = Message(channel="test_channel")
        message.send()
        mock_logger.error.assert_called_once_with(
            "Error sending message to Slack: %s", "some_error"
        )


class TestSendErrorMessage:
    """Test the send_error_message function."""

    def test_send_error_message_content(
        self,
        mock_settings,  # pylint: disable=unused-argument
        slack_channel,
        sample_error,
        mock_message_class,
    ):
        """Test the content of the error message."""
        send_error_message(slack_channel, sample_error)
        mock_message_class.assert_called_once()
        call_args = mock_message_class.call_args
        channel_arg = call_args[1]["channel"]
        blocks_arg = call_args[1]["blocks"].blocks

        assert channel_arg == slack_channel
        assert "Oops! Something went wrong. :thumbsdown:" in blocks_arg[0].text.text
        assert (
            "An error has occurred during 'TestApp' processing for TestName."
            in blocks_arg[1].elements[0].elements[0].text
        )
        assert (
            f"{sample_error.__class__.__name__}: {sample_error}"
            in blocks_arg[1].elements[1].elements[0].text
        )

    def test_send_error_message_different_errors(
        self,
        mock_settings,  # pylint: disable=unused-argument
        slack_channel,
        mock_message_class,
    ):
        """Test sending different types of errors."""
        errors = [
            ValueError("Value error"),
            KeyError("Key error"),
            Exception("General exception"),
        ]
        for error in errors:
            send_error_message(slack_channel, error)
            call_args = mock_message_class.call_args
            error_text = call_args[1]["blocks"].blocks[1].elements[1].elements[0].text
            assert f"{error.__class__.__name__}: {error}" in error_text
            mock_message_class.reset_mock()


class TestSendNotification:
    """Test the send_notification function."""

    def test_send_notification_single_line(self, mock_message_class):
        """Test sending a single line notification."""
        channel = "#general"
        line = "This is a test message."
        send_notification(channel, line)
        mock_message_class.assert_called_once()
        _, kwargs = mock_message_class.call_args
        assert kwargs["channel"] == channel
        assert "Hello there! :wave:" in str(kwargs["blocks"])
        assert line in str(kwargs["blocks"])

    def test_send_notification_multiple_lines(self, mock_message_class):
        """Test sending a multi-line notification."""
        channel = "#general"
        lines = ["First line", "Second line"]
        send_notification(channel, *lines)
        mock_message_class.assert_called_once()
        _, kwargs = mock_message_class.call_args
        assert kwargs["channel"] == channel
        assert all(line in str(kwargs["blocks"]) for line in lines)

    def test_send_notification_markdown_formatting(self, mock_message_class):
        """Test sending a notification with markdown formatting."""
        channel = "#general"
        line = "*Bold text* _italic text_"
        send_notification(channel, line)
        mock_message_class.assert_called_once()
        _, kwargs = mock_message_class.call_args
        assert kwargs["channel"] == channel
        assert line in str(kwargs["blocks"])

    def test_send_notification_message_sending(self, mock_message_class):
        """Test sending a notification message."""
        channel = "#general"
        line = "Test message."
        send_notification(channel, line)
        mock_message_class.return_value.send.assert_called_once()
