"""Optional Telegram notifications for Chronicling America bulk downloads."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

import requests

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"
DEFAULT_ENV_FILE = ".env"


def load_dotenv_file(path: str = DEFAULT_ENV_FILE) -> None:
    """Load KEY=VALUE pairs from a dotenv file into os.environ (without overwriting)."""
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


class TelegramNotifier:
    """Send status messages via a Telegram bot (non-blocking, best-effort)."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        self._chat_id = chat_id
        self._timeout = timeout
        self._session = session or requests.Session()
        self._send_url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, env_file: str = DEFAULT_ENV_FILE) -> TelegramNotifier | None:
        load_dotenv_file(env_file)
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            if token and not chat_id:
                logger.warning(
                    "TELEGRAM_BOT_TOKEN is set but TELEGRAM_CHAT_ID is missing; "
                    "Telegram notifications disabled"
                )
            return None
        return cls(token, chat_id)

    def _post_async(self, text: str) -> None:
        thread = threading.Thread(
            target=self._post,
            args=(text,),
            name="ca-telegram-notify",
            daemon=True,
        )
        thread.start()

    def _post(self, text: str) -> None:
        payload: dict[str, Any] = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            with self._lock:
                response = self._session.post(
                    self._send_url,
                    json=payload,
                    timeout=self._timeout,
                )
                response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Telegram notification failed: %s", exc)

    def notify_batch_complete(
        self,
        batch: str,
        *,
        issues_finalized: int,
        tarball_size: int | None = None,
    ) -> None:
        size_note = f" ({tarball_size} bytes)" if tarball_size else ""
        self._post_async(
            f"CA download: batch {batch} finished{size_note}. "
            f"{issues_finalized} issue(s) finalized."
        )

    def notify_captcha(self, url: str, *, sleep_seconds: float) -> None:
        self._post_async(
            "CA download: CAPTCHA/challenge detected.\n"
            f"URL: {url}\n"
            f"Retrying after {sleep_seconds:.0f}s sleep."
        )
