import hashlib
import io

import pytest

from scrapy_webarchive.utils import BUFF_SIZE, hash_stream


def test_hash_stream_with_empty_stream():
    # Test with an empty stream
    data = b""
    stream = io.BytesIO(data)
    size, result = hash_stream("sha256", stream)
    
    assert size == 0
    assert result == f"sha256:{hashlib.sha256(data).hexdigest()}"

def test_hash_stream_with_md5_algorithm():
    data = b"Hello world"
    expected_hash = hashlib.md5(data).hexdigest()
    
    stream = io.BytesIO(data)
    size, result = hash_stream("md5", stream)
    
    assert size == len(data)
    assert result == f"md5:{expected_hash}"

def test_hash_stream_with_sha256_algorithm():
    data = b"Hello world"
    expected_hash = hashlib.sha256(data).hexdigest()
    
    stream = io.BytesIO(data)
    size, result = hash_stream("sha256", stream)
    
    assert size == len(data)
    assert result == f"sha256:{expected_hash}"

def test_hash_stream_with_unsupported_hash_type():
    data = b"Hello world"
    stream = io.BytesIO(data)

    with pytest.raises(ValueError):
        hash_stream("unsupported_hash", stream)

def test_hash_stream_with_large_stream():
    data = b"a" * (2 * BUFF_SIZE)  # Twice the buffer size
    expected_hash = hashlib.sha256(data).hexdigest()
    
    stream = io.BytesIO(data)
    size, result = hash_stream("sha256", stream)
    
    assert size == len(data)
    assert result == f"sha256:{expected_hash}"
