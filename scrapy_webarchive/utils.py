from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Tuple
from urllib.parse import urlparse

WARC_DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIMESTAMP_DT_FORMAT = "%Y%m%d%H%M%S"
BUFF_SIZE = 1024 * 64

logger = logging.getLogger(__name__)

def get_formatted_dt_string(format: str) -> str:
    return datetime.now(timezone.utc).strftime(format)


def header_lines_to_dict(lines):
    # TODO: Only supports each header appearing once, not multiple occurences.
    headers = {}
    for line in lines:
        k, v = line.split(b":", 1)
        v = v.lstrip()
        headers[k] = v
    return headers


def get_scheme_from_uri(uri: str) -> str:
    if Path(uri).is_absolute():  # to support win32 paths like: C:\\some\dir.
        return "file"
    else:
        return urlparse(uri).scheme


def hash_stream(hash_type: str, stream: IO) -> Tuple[int, str]:
    """Hashes the stream with given hash_type hasher."""

    # At this moment the `hash_type` (or algorithm) that we pass will always be sha256 as it is hardcoded.
    # This check is implemented in case any other algorithms will be made available in the future.
    if hash_type not in hashlib.algorithms_guaranteed:
        raise ValueError(f"Unsupported hash type: {hash_type}")

    hasher = hashlib.new(hash_type)

    size = 0
    for chunk in iter(lambda: stream.read(BUFF_SIZE), b""):
        size += len(chunk)
        hasher.update(chunk)

    return size, f"{hash_type}:{hasher.hexdigest()}"
