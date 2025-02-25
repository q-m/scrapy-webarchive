import os
import re
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from freezegun import freeze_time

from scrapy_webarchive.models import FileInfo
from scrapy_webarchive.resolvers import LocalFileResolver, S3FileResolver, create_resolver


@pytest.fixture
def mock_s3_client():
    return Mock()


@pytest.fixture
def sample_regex():
    return re.compile(r".*\.wacz$")


class TestS3FileResolver:
    def test_resolve_with_matching_files(self, mock_s3_client, sample_regex):
        mock_response = {
            "Contents": [
                {"Key": "archive_1.wacz", "LastModified": datetime(2025, 1, 1)},
                {"Key": "archive_2.warc", "LastModified": datetime(2025, 1, 2)},
                {"Key": "archive_3.wacz", "LastModified": datetime(2025, 1, 3)},
            ]
        }
        mock_s3_client.list_objects_v2.return_value = mock_response

        resolver = S3FileResolver(mock_s3_client, "scrapy-webarchive", sample_regex)
        result = resolver.resolve()

        expected = [
            FileInfo(uri="s3://scrapy-webarchive/archive_1.wacz", last_modified=datetime(2025, 1, 1).timestamp()),
            FileInfo(uri="s3://scrapy-webarchive/archive_3.wacz", last_modified=datetime(2025, 1, 3).timestamp()),
        ]
        
        assert result == expected
        mock_s3_client.list_objects_v2.assert_called_once_with(Bucket="scrapy-webarchive")

    def test_resolve_no_contents(self, mock_s3_client, sample_regex):
        mock_s3_client.list_objects_v2.return_value = {}
        resolver = S3FileResolver(mock_s3_client, "scrapy-webarchive", sample_regex)

        result = resolver.resolve()
        assert result == []


class TestLocalFileResolver:
    @freeze_time("2025-01-01 12:00:00")
    def test_resolve_with_matching_files(self, sample_regex, fs):
        base_path = "/scrapy-webarchive"
        fs.create_file(os.path.join(base_path, "archive_1.wacz"), contents="")
        fs.create_file(os.path.join(base_path, "archive_2.warc"), contents="")
        fs.create_file(os.path.join(base_path, "archive_3.wacz"), contents="")

        resolver = LocalFileResolver(base_path, sample_regex)
        result = resolver.resolve()

        expected_last_modified = datetime(2025, 1, 1, 12).timestamp() # frozen time
        expected = [
            FileInfo(uri="file:///scrapy-webarchive/archive_1.wacz", last_modified=expected_last_modified),
            FileInfo(uri="file:///scrapy-webarchive/archive_3.wacz", last_modified=expected_last_modified),
        ]
        assert result == expected


class TestCreateResolver:
    @patch('scrapy_webarchive.resolvers.get_s3_client')
    def test_create_s3_resolver(self, mock_get_s3_client, sample_regex):
        mock_s3_client = Mock()
        mock_get_s3_client.return_value = mock_s3_client

        resolver = create_resolver({}, "s3://scrapy-webarchive/quotes/", sample_regex)

        assert isinstance(resolver, S3FileResolver)
        assert resolver.bucket == "scrapy-webarchive"
        assert resolver.s3_client == mock_s3_client
        assert resolver.pattern_regex.pattern == "quotes/.*\\.wacz$"

    def test_create_local_resolver(self, sample_regex):
        resolver = create_resolver({}, "/scrapy-webarchive", sample_regex)

        assert isinstance(resolver, LocalFileResolver)
        assert resolver.base_path == Path("/scrapy-webarchive")
        assert resolver.pattern_regex == sample_regex
