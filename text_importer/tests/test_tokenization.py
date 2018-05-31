from text_importer.tokenization import insert_whitespace


def test_insert_whitespace():
    assert insert_whitespace("Lausanne", ",", None, "fr") is False
