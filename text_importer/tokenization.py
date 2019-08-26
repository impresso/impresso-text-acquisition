"""Tokenization rules for various languages."""

import logging

logger = logging.getLogger(__name__)

WHITESPACE_RULES = {
    "fr": {
        "punctuation_nows_before": [
            ".",
            ",",
            ")",
            "]",
            "}",
            "°",
            "..."
        ],
        "punctuation_nows_after": ["(", "[", "{"],
        "punctuation_nows_beforeafter": ["'", "-"],
        "punctuation_ciffre": [".", ","]
    }
}


def insert_whitespace(token, following_token, previous_token, language):
    """Determine whether a whitespace should be inserted after a token."""
    try:
        wsrules = WHITESPACE_RULES[language]
    except Exception:
        pass
        return

    insert_ws = True

    if (
        token in wsrules["punctuation_nows_beforeafter"] or
        following_token in wsrules["punctuation_nows_beforeafter"]
    ):
        insert_ws = False

    elif following_token in wsrules["punctuation_nows_before"]:
        insert_ws = False

    elif token in wsrules["punctuation_nows_after"]:
        insert_ws = False

    elif (
            token in wsrules["punctuation_ciffre"] and
            previous_token is not None and
            following_token is not None
    ):
        if previous_token.isdigit() and following_token.isdigit():
            return False
        else:
            return True

    logger.debug(
        f"Insert whitespace: curr={token}, follow={following_token}, \
            prev={previous_token} ({insert_ws})"
    )
    return insert_ws
