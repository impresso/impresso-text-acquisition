"""Tests for Chronicling America Telegram notifications."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from text_preparation.importers.chronicling_america.telegram_notify import (
    TelegramNotifier,
    load_dotenv_file,
)


def test_load_dotenv_file_sets_missing_keys(tmp_path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        'TELEGRAM_BOT_TOKEN="abc123"\n'
        "# comment\n"
        "TELEGRAM_CHAT_ID=999\n",
        encoding="utf-8",
    )
    os.environ.pop("TELEGRAM_BOT_TOKEN", None)
    os.environ.pop("TELEGRAM_CHAT_ID", None)

    load_dotenv_file(str(env_path))

    assert os.environ["TELEGRAM_BOT_TOKEN"] == "abc123"
    assert os.environ["TELEGRAM_CHAT_ID"] == "999"


def test_from_env_returns_none_without_chat_id(tmp_path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("TELEGRAM_BOT_TOKEN=abc\n", encoding="utf-8")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    assert TelegramNotifier.from_env(str(env_path)) is None


def test_from_env_builds_notifier(tmp_path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "TELEGRAM_BOT_TOKEN=abc\nTELEGRAM_CHAT_ID=12345\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    notifier = TelegramNotifier.from_env(str(env_path))
    assert notifier is not None
    assert notifier._chat_id == "12345"


def test_notify_batch_complete_posts_message() -> None:
    session = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None
    session.post.return_value = response
    notifier = TelegramNotifier("token", "chat-id", session=session)

    with patch.object(notifier, "_post_async", side_effect=notifier._post):
        notifier.notify_batch_complete(
            "dlc_test_ver01",
            issues_finalized=3,
            tarball_size=1024,
        )

    session.post.assert_called_once()
    payload = session.post.call_args.kwargs["json"]
    assert payload["chat_id"] == "chat-id"
    assert "dlc_test_ver01" in payload["text"]
    assert "3 issue(s)" in payload["text"]


def test_notify_captcha_posts_message() -> None:
    session = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None
    session.post.return_value = response
    notifier = TelegramNotifier("token", "chat-id", session=session)

    with patch.object(notifier, "_post_async", side_effect=notifier._post):
        notifier.notify_captcha("http://example/page", sleep_seconds=3600.0)

    payload = session.post.call_args.kwargs["json"]
    assert "CAPTCHA/challenge" in payload["text"]
    assert "http://example/page" in payload["text"]


def test_http_client_notifies_on_captcha() -> None:
    from text_preparation.importers.chronicling_america.bulk import HttpClient

    bad_response = MagicMock(
        status_code=403,
        headers={"Content-Type": "text/html"},
    )
    bad_response.text = "<html>Just a moment...</html>"
    good_response = MagicMock(status_code=200)
    session = MagicMock()
    session.get.side_effect = [bad_response, good_response]

    notifier = MagicMock()
    client = HttpClient(delay=0, session=session, notifier=notifier)

    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        client.request("http://example/item")

    notifier.notify_captcha.assert_called_once()
    assert notifier.notify_captcha.call_args.kwargs["sleep_seconds"] > 0


def test_post_logs_warning_on_failure(caplog) -> None:
    session = MagicMock()
    session.post.side_effect = requests.ConnectionError("offline")
    notifier = TelegramNotifier("token", "chat-id", session=session)

    with caplog.at_level("WARNING"):
        notifier._post("hello")

    assert "Telegram notification failed" in caplog.text
