from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

from scrapy.settings import Settings
from typing_extensions import List, Protocol

from scrapy_webarchive.models import FileInfo
from scrapy_webarchive.wacz.storages import get_s3_client


class FileResolver(Protocol):
    def resolve(self) -> List[FileInfo]:
        ...


class S3FileResolver:
    def __init__(self, s3_client, bucket: str, regex_pattern: re.Pattern):
        self.s3_client = s3_client
        self.bucket = bucket
        self.pattern_regex = regex_pattern

    def resolve(self) -> List[FileInfo]:
        """Resolve files from S3 matching the given pattern."""

        objects = self.s3_client.list_objects_v2(Bucket=self.bucket)
        return [
            FileInfo(uri=f"s3://{self.bucket}/{obj['Key']}", last_modified=obj['LastModified'].timestamp())
            for obj in objects.get("Contents", [])
            if self.pattern_regex.fullmatch(obj['Key'])
        ]


class LocalFileResolver:
    def __init__(self, base_path: str, regex_pattern: re.Pattern):
        self.base_path = Path(base_path)
        self.pattern_regex = regex_pattern

    def resolve(self) -> List[FileInfo]:
        """Resolve local files based on regex pattern."""

        return [
            FileInfo(uri=f'file://{str(path)}', last_modified=path.stat().st_mtime)
            for path in self.base_path.rglob("*")
            if self.pattern_regex.fullmatch(str(path.relative_to(self.base_path))) and not path.is_dir()
        ]


def create_resolver(settings: Settings, base_path: str, regex_pattern: re.Pattern) -> FileResolver:
    """Factory function to create the appropriate resolver based on the URI template."""

    if base_path.startswith("s3://"):
        s3_client = get_s3_client(settings)
        parse_result = urlparse(base_path, allow_fragments=False)
        regex_pattern = re.compile(parse_result.path.lstrip('/') + regex_pattern.pattern)
        return S3FileResolver(s3_client=s3_client, bucket=parse_result.netloc, regex_pattern=regex_pattern)
    else:
        return LocalFileResolver(base_path=base_path, regex_pattern=regex_pattern)
