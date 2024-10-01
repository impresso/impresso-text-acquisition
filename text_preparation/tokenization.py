"""Tokenization rules for various languages."""

import logging
from impresso_essentials.text_utils import WHITESPACE_RULES

logger = logging.getLogger(__name__)


def insert_whitespace(
    token: str, following_token: str, previous_token: str, language: str
) -> bool:
    """Determine whether a whitespace should be inserted after a token.

    Args:
        token (str): Current token.
        following_token (str): Following token.
        previous_token (str): Previous token.
        language (str): Language of text.

    Returns:
        bool: Whether a whitespace should be inserted after the `token`.
    """
    try:
        wsrules = WHITESPACE_RULES[language]
    except Exception:
        pass
        return

    insert_ws = True

    if (
        token in wsrules["pct_no_ws_before_after"]
        or following_token in wsrules["pct_no_ws_before_after"]
    ):
        insert_ws = False

    elif following_token in wsrules["pct_no_ws_before"]:
        insert_ws = False

    elif token in wsrules["pct_no_ws_after"]:
        insert_ws = False

    elif (
        token in wsrules["pct_number"]
        and previous_token is not None
        and following_token is not None
    ):
        if previous_token.isdigit() and following_token.isdigit():
            return False
        else:
            return True

    logger.debug(
        "Insert whitespace: curr=%s, follow=%s, prev=%s (%s)",
        token,
        following_token,
        previous_token,
        insert_ws,
    )
    return insert_ws
