import pytest

from shared.text_extraction import UnsupportedFileType, doc_id_from_filename, extract_text


def test_markdown_and_whitespace_normalization():
    text = extract_text("notes.md", "# Title\r\n\r\n\r\n\r\nbody   with\t\tspaces".encode())
    assert text == "# Title\n\nbody with spaces"


def test_utf8_bom_and_unknown_type():
    assert extract_text("a.txt", "﻿hello".encode("utf-8")) == "hello"
    with pytest.raises(UnsupportedFileType):
        extract_text("photo.png", b"\x89PNG\r\n")


def test_doc_id_from_filename():
    assert doc_id_from_filename("My Notes (v2).pdf") == "my-notes-v2"
    assert doc_id_from_filename("???.txt") == "document"
