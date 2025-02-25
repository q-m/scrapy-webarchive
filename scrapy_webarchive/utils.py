from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from typing_extensions import IO, Dict, Optional, Tuple

from scrapy_webarchive.constants import BUFF_SIZE

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


def parse_iso8601_datetime(time_str: str) -> Optional[datetime]:
    """Convert the target ISO time string to a datetime object."""

    if not time_str:
        return None

    try:
        return datetime.fromisoformat(time_str)
    except ValueError:
        raise ValueError(f"Invalid date format: {time_str}. Use ISO format (YYYY-MM-DDTHH:MM:SS).")


def get_archive_uri_template_dt_variables() -> dict:
    current_date = datetime.now()

    return {
        "year": current_date.strftime("%Y"),
        "month": current_date.strftime("%m"),
        "day": current_date.strftime("%d"),
        "timestamp": current_date.strftime("%Y%m%d%H%M%S"),
    }


def get_placeholder_patterns(spider_name: str) -> Dict[str, str]:
    """Return a mapping of placeholders to their corresponding regex patterns."""

    return {
        "{year}": r"[0-9]{4}",
        "{month}": r"[0-9]{2}",
        "{day}": r"[0-9]{2}",
        "{timestamp}": r"[0-9]+",
        "{spider}": spider_name,
        "{filename}": r"[^/\\]+\.wacz$",
    }


def is_uri_directory(uri: str) -> bool:
    """Check if the URI is a directory or a file."""

    parsed = urlparse(uri)
    scheme = parsed.scheme
    path = parsed.path

    if not scheme and uri.startswith("/"):
        path = uri

    last_part = path.rstrip("/").split("/")[-1]

    if path.endswith("/"):
        return True
    elif "." in last_part:
        return False
    else:
        return True


def extract_base_from_uri_template(uri_template: str) -> str:
    """Extract the static base path from a URI template before the first placeholder."""

    first_placeholder_pos = uri_template.find("{")
    if first_placeholder_pos == -1:
        return uri_template

    return uri_template[:first_placeholder_pos]


def build_regex_pattern(uri_template: str, placeholder_patterns: Dict[str, str]) -> re.Pattern:
    """Build and compile a regex pattern from a URI template with placeholders."""

    first_placeholder_pos = uri_template.find("{")
    pattern_str = uri_template[first_placeholder_pos:]
    for placeholder, regex in placeholder_patterns.items():
        pattern_str = pattern_str.replace(placeholder, regex)

    return re.compile(pattern_str)
