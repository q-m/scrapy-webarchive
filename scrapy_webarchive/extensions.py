from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from scrapy import Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.pipelines.files import FSFilesStore, FTPFilesStore, GCSFilesStore, S3FilesStore
from scrapy.settings import Settings
from typing_extensions import Self

from scrapy_webarchive.utils import warc_date
from scrapy_webarchive.wacz import WaczFileCreator
from scrapy_webarchive.warc import WarcFileWriter


class WaczExporter:
    """WACZ exporter extension that writes spider requests/responses as WACZ during a crawl job."""

    STORE_SCHEMES = {
        "": FSFilesStore,
        "file": FSFilesStore,
        "s3": S3FilesStore,
        "gs": GCSFilesStore,
        "ftp": FTPFilesStore,
    }

    def __init__(self, settings: Settings, crawler: Crawler) -> None:
        self.settings = settings
        self.stats = crawler.stats

        if not self.settings["ARCHIVE_EXPORT_URI"]:
            raise NotConfigured

        self.store = self._get_store()
        self.writer = WarcFileWriter(collection_name=crawler.spider.name)

    def _get_store(self):
        archive_uri_template = self.settings["ARCHIVE_EXPORT_URI"]
        uri = archive_uri_template.format(**get_archive_uri_template_variables())
    
        if Path(uri).is_absolute():  # to support win32 paths like: C:\\some\dir
            scheme = "file"
        else:
            scheme = urlparse(uri).scheme

        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(uri)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        assert crawler.stats

        try:
            exporter = cls.from_settings(crawler.settings, crawler)
        except AttributeError:
            exporter = cls(crawler.settings, crawler)

        crawler.signals.connect(exporter.response_received, signal=signals.response_received)
        crawler.signals.connect(exporter.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(exporter.spider_opened, signal=signals.spider_opened)
        return exporter
    
    @classmethod
    def from_settings(cls, settings: Settings, crawler: Crawler):
        s3store = cls.STORE_SCHEMES["s3"]
        s3store.AWS_ACCESS_KEY_ID = settings["AWS_ACCESS_KEY_ID"]
        s3store.AWS_SECRET_ACCESS_KEY = settings["AWS_SECRET_ACCESS_KEY"]
        s3store.AWS_SESSION_TOKEN = settings["AWS_SESSION_TOKEN"]
        s3store.AWS_ENDPOINT_URL = settings["AWS_ENDPOINT_URL"]
        s3store.AWS_REGION_NAME = settings["AWS_REGION_NAME"]
        s3store.AWS_USE_SSL = settings["AWS_USE_SSL"]
        s3store.AWS_VERIFY = settings["AWS_VERIFY"]
        s3store.POLICY = settings["FILES_STORE_S3_ACL"]

        gcs_store = cls.STORE_SCHEMES["gs"]
        gcs_store.GCS_PROJECT_ID = settings["GCS_PROJECT_ID"]
        gcs_store.POLICY = settings["FILES_STORE_GCS_ACL"] or None

        ftp_store = cls.STORE_SCHEMES["ftp"]
        ftp_store.FTP_USERNAME = settings["FTP_USER"]
        ftp_store.FTP_PASSWORD = settings["FTP_PASSWORD"]
        ftp_store.USE_ACTIVE_MODE = settings.getbool("FEED_STORAGE_FTP_ACTIVE")

        return cls(settings=settings, crawler=crawler)

    def spider_opened(self) -> None:
        self.writer.write_warcinfo(robotstxt_obey=self.settings["ROBOTSTXT_OBEY"])

    def response_received(self, response: Response, request: Request, spider: Spider) -> None:
        request.meta["WARC-Date"] = warc_date()

        # Write response WARC record
        record = self.writer.write_response(response, request)
        self.stats.inc_value("wacz/exporter/response_written", spider=spider)
        self.stats.inc_value(
            f"wacz/exporter/writer_status_count/{record.http_headers.get_statuscode()}", 
            spider=spider,
        )

        # Write request WARC record
        self.writer.write_request(request, concurrent_to=record)
        self.stats.inc_value("wacz/exporter/request_written", spider=spider)

    def spider_closed(self) -> None:
        WaczFileCreator(warc_fname=self.writer.warc_fname, store=self.store).create()


def get_archive_uri_template_variables() -> dict:
    current_date = datetime.now()

    return {
        "year": current_date.strftime("%Y"),
        "month": current_date.strftime("%m"),
        "day": current_date.strftime("%d"),
        "timestamp": current_date.strftime("%Y%m%d%H%M%S"),
    }
