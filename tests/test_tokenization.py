from text_preparation.tokenization import insert_whitespace


def test_insert_whitespace():
    assert insert_whitespace("Lausanne", ",", None, "fr") is False
    assert insert_whitespace("(", "encore", None, "fr") is False
    assert insert_whitespace(".", "01", "52", "fr") is False
    assert insert_whitespace(",", "500", "000", "fr") is False
    assert insert_whitespace(",", "500", None, "fr") is True
