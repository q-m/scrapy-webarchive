from unittest import mock

import pytest
from scrapy.exceptions import NotConfigured
from scrapy.pipelines.files import FSFilesStore, FTPFilesStore, GCSFilesStore, S3FilesStore
from scrapy.utils.test import get_crawler

from scrapy_webarchive.extensions import WaczExporter


class TestWaczExporterExtension:
    def test_archive_export_uri_invalid_raises_not_configured(self):
        crawler = get_crawler(settings_dict={})
        with pytest.raises(NotConfigured):
            WaczExporter.from_crawler(crawler)

    @mock.patch('scrapy_webarchive.extensions.S3FilesStore.__init__', return_value=None)
    @mock.patch('scrapy_webarchive.extensions.GCSFilesStore.__init__', return_value=None)
    @mock.patch('scrapy_webarchive.extensions.FTPFilesStore.__init__', return_value=None)
    @mock.patch('scrapy_webarchive.extensions.FSFilesStore.__init__', return_value=None)
    def test_get_store(self, *args):
        crawler = get_crawler(settings_dict={"ARCHIVE_EXPORT_URI": "/tmp/scrapy-webarchive/wacz/"})
        crawler.spider = crawler._create_spider("quotes")
        extension = WaczExporter.from_crawler(crawler)
        assert isinstance(extension.store, FSFilesStore)
    
        crawler = get_crawler(settings_dict={"ARCHIVE_EXPORT_URI": "s3://scrapy-webarchive/wacz/"})
        crawler.spider = crawler._create_spider("quotes")
        extension = WaczExporter.from_crawler(crawler)
        assert isinstance(extension.store, S3FilesStore)

        crawler = get_crawler(settings_dict={"ARCHIVE_EXPORT_URI": "gs://scrapy-webarchive/wacz/"})
        crawler.spider = crawler._create_spider("quotes")
        extension = WaczExporter.from_crawler(crawler)
        assert isinstance(extension.store, GCSFilesStore)

        crawler = get_crawler(settings_dict={"ARCHIVE_EXPORT_URI": "ftp://scrapy-webarchive/wacz/"})
        crawler.spider = crawler._create_spider("quotes")
        extension = WaczExporter.from_crawler(crawler)
        assert isinstance(extension.store, FTPFilesStore)
