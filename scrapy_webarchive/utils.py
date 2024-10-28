from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Tuple
from urllib.parse import urlparse, urlunparse

from scrapy.settings import Settings

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


def get_s3_client(settings: Settings):
    """Create an S3 client using the given settings."""

    import botocore.session
    session = botocore.session.get_session()
    return session.create_client(
        "s3",
        aws_access_key_id=settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=settings["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=settings["AWS_SESSION_TOKEN"],
        endpoint_url=settings["AWS_ENDPOINT_URL"],
        region_name=settings["AWS_REGION_NAME"],
        use_ssl=settings["AWS_USE_SSL"],
        verify=settings["AWS_VERIFY"],
    )


def get_gcs_client(settings: Settings):
    """Create a Google Cloud Storage client using the given settings."""

    from google.cloud import storage
    return storage.Client(project=settings["GCS_PROJECT_ID"])


def add_ftp_credentials(wacz_uri: str, settings: Settings) -> str:
    """Add FTP username and password to the URI if not present."""

    parsed_uri = urlparse(wacz_uri)

    if parsed_uri.username is None:
        # Build netloc with credentials.
        credentials = f'{settings["FTP_USER"]}:{settings["FTP_PASSWORD"]}'
        netloc = f'{credentials}@{parsed_uri.hostname}'
        
        # Add port if present.
        if parsed_uri.port:
            netloc += f":{parsed_uri.port}"
        
        # Update and return the URI with credentials.
        updated_uri = parsed_uri._replace(netloc=netloc)
        return urlunparse(updated_uri)
    
    return wacz_uri


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
