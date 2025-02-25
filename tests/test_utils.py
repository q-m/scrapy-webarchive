import hashlib
import io
import re

import pytest

from scrapy_webarchive import utils
from scrapy_webarchive.constants import BUFF_SIZE


def test_hash_stream_with_empty_stream():
    data = b""
    stream = io.BytesIO(data)
    size, result = utils.hash_stream("sha256", stream)
    
    assert size == 0
    assert result == f"sha256:{hashlib.sha256(data).hexdigest()}"


def test_hash_stream_with_md5_algorithm():
    data = b"Hello world"
    expected_hash = hashlib.md5(data).hexdigest()
    
    stream = io.BytesIO(data)
    size, result = utils.hash_stream("md5", stream)
    
    assert size == len(data)
    assert result == f"md5:{expected_hash}"


def test_hash_stream_with_sha256_algorithm():
    data = b"Hello world"
    expected_hash = hashlib.sha256(data).hexdigest()
    
    stream = io.BytesIO(data)
    size, result = utils.hash_stream("sha256", stream)
    
    assert size == len(data)
    assert result == f"sha256:{expected_hash}"


def test_hash_stream_with_unsupported_hash_type():
    data = b"Hello world"
    stream = io.BytesIO(data)

    with pytest.raises(ValueError):
        utils.hash_stream("unsupported_hash", stream)


def test_hash_stream_with_large_stream():
    data = b"a" * (2 * BUFF_SIZE)  # Twice the buffer size
    expected_hash = hashlib.sha256(data).hexdigest()
    
    stream = io.BytesIO(data)
    size, result = utils.hash_stream("sha256", stream)
    
    assert size == len(data)
    assert result == f"sha256:{expected_hash}"


@pytest.mark.parametrize("uri, expected", [
    # Directories
    ("s3://scrapy-webarchive/quotes/", True),
    ("s3://scrapy-webarchive/quotes", True),
    ("/scrapy/webarchive/", True),
    ("scrapy/webarchive/", True),
    ("/scrapy-webarchive", True),

    # Files
    ("s3://scrapy-webarchive/quotes/archive.wacz", False),
    ("s3://scrapy-webarchive/archive.wacz", False),
    ("/scrapy/webarchive/archive.wacz", False),
])
def test_is_uri_directory(uri: str, expected: bool):
    assert utils.is_uri_directory(uri) == expected


@pytest.mark.parametrize("uri_template, expected", [
    ("s3://scrapy-webarchive/{year}/quotes/{day}/res-{timestamp}.wacz", "s3://scrapy-webarchive/"),
    ("s3://scrapy-webarchive/quotes/{year}/{month}/{day}/res-{timestamp}.wacz", "s3://scrapy-webarchive/quotes/"),
    ("s3://scrapy-webarchive/", "s3://scrapy-webarchive/"),
    ("file:///scrapy/{year}/quotes/{day}/res-{timestamp}.wacz", "file:///scrapy/"),
    ("file:///scrapy/webarchive/{year}/", "file:///scrapy/webarchive/"),
    ("file:///scrapy/webarchive/", "file:///scrapy/webarchive/"),
    ("/scrapy/{year}/quotes/{day}/res-{timestamp}.wacz", "/scrapy/"),
    ("/scrapy/webarchive/{spider}/", "/scrapy/webarchive/"),
    ("/scrapy/webarchive/", "/scrapy/webarchive/"),
])
def test_extract_base_from_uri_template(uri_template: str, expected: str):
    result = utils.extract_base_from_uri_template(uri_template)
    assert result == expected


def test_build_regex_pattern_with_placeholders():
    uri_template = "s3://scrapy-webarchive/{spider}/{year}/{month}/{day}/{timestamp}/"
    result = utils.build_regex_pattern(uri_template, utils.get_placeholder_patterns(spider_name="quotes"))

    assert result.pattern == "quotes/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]+/"
    assert isinstance(result, re.Pattern)

    assert result.match("quotes/2025/01/01/1735686000/")
    assert not result.match("quotes/01/01/2025/1735686000/") # Year is not first
