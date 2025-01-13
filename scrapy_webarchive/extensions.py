from __future__ import annotations

import os
from datetime import datetime
from io import BytesIO
from typing import Tuple

from scrapy import Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.pipelines.files import FSFilesStore, FTPFilesStore, GCSFilesStore, S3FilesStore
from scrapy.pipelines.media import MediaPipeline
from scrapy.settings import Settings
from twisted.internet.defer import Deferred
from typing_extensions import Any, Dict, Protocol, Self, Type, Union, cast

from scrapy_webarchive.utils import WARC_DT_FORMAT, get_formatted_dt_string, get_scheme_from_uri
from scrapy_webarchive.wacz.creator import WaczFileCreator
from scrapy_webarchive.warc import WarcFileWriter


class FilesStoreProtocol(Protocol):
    # This protocol will be introduced by Scrapy at a later stage. Once that happens we can drop this bit.
    # For now we implement it ourselves to improve type hints for the stores.
    def __init__(self, basedir: str): 
        ...

    def persist_file(
        self,
        path: str,
        buf: BytesIO,
        info: MediaPipeline.SpiderInfo,
        meta: Union[Dict[str, Any], None] = None,
        headers: Union[Dict[str, str], None] = None,
    ) -> Union[Deferred[Any], None]: 
        ...


class WaczExporter:
    """WACZ exporter extension that writes spider requests/responses as WARC and later compiles them to a WACZ."""

    wacz_fname = None
    STORE_SCHEMES: Dict[str, Type[FilesStoreProtocol]] = {
        "": FSFilesStore,
        "file": FSFilesStore,
        "s3": S3FilesStore,
        "gs": GCSFilesStore,
        "ftp": FTPFilesStore,
    }

    def __init__(self, settings: Settings, crawler: Crawler) -> None:
        self.settings = settings
        
        # Check configuration prerequisites
        self._check_configuration_prerequisites()

        self.stats = crawler.stats
        self.crawler = crawler
        self.spider_name = crawler.spidercls.name if hasattr(crawler.spidercls, "name") else crawler.spider.name

        # Get the store URI and configure the WACZ filename
        store_uri, self.wacz_fname  = self._retrieve_store_uri_and_wacz_fname()

        # Initialize store and writer
        self.store: FilesStoreProtocol = self._get_store(store_uri)
        self.writer = WarcFileWriter(collection_name=self.spider_name)

    def _check_configuration_prerequisites(self) -> None:
        """raises NotConfigured if essential settings or middleware configurations are incorrect."""
        
        if not self.settings.get("SW_EXPORT_URI"):
            raise NotConfigured("Missing SW_EXPORT_URI setting.")
        
        if self.settings.get("SW_WACZ_SOURCE_URI"):
            raise NotConfigured("WACZ exporter is disabled when scraping from a WACZ archive.")

    def _retrieve_store_uri_and_wacz_fname(self) -> Tuple[str, Union[str, None]]:
        """Sets up the export URI based on configuration and spider context."""

        export_uri = self.settings["SW_EXPORT_URI"].format(
            spider=self.spider_name,
            **get_archive_uri_template_dt_variables(),
        )

        if os.path.isdir(export_uri):
            return export_uri, None
        else:
            export_uri, wacz_fname = os.path.split(export_uri)
            return f"{export_uri}/", wacz_fname

    def _get_store(self, store_uri: str) -> FilesStoreProtocol:
        store_cls = self.STORE_SCHEMES[get_scheme_from_uri(store_uri)]
        return store_cls(store_uri)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        assert crawler.stats

        try:
            exporter = cls.from_settings(crawler.settings, crawler)
        except AttributeError:
            exporter = cls(crawler.settings, crawler)

        crawler.signals.connect(exporter.response_downloaded, signal=signals.response_downloaded)
        crawler.signals.connect(exporter.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(exporter.spider_opened, signal=signals.spider_opened)
        return exporter
    
    @classmethod
    def from_settings(cls, settings: Settings, crawler: Crawler):
        """
        Store configuration is based on the images/files pipeline from Scrapy. See:
        https://github.com/scrapy/scrapy/blob/d4709e41047e794c9e39968f61c5abbcddf825c4/scrapy/pipelines/images.py#L94-L116
        """

        s3store: Type[S3FilesStore] = cast(Type[S3FilesStore], cls.STORE_SCHEMES["s3"])
        s3store.AWS_ACCESS_KEY_ID = settings["AWS_ACCESS_KEY_ID"]
        s3store.AWS_SECRET_ACCESS_KEY = settings["AWS_SECRET_ACCESS_KEY"]
        s3store.AWS_SESSION_TOKEN = settings["AWS_SESSION_TOKEN"]
        s3store.AWS_ENDPOINT_URL = settings["AWS_ENDPOINT_URL"]
        s3store.AWS_REGION_NAME = settings["AWS_REGION_NAME"]
        s3store.AWS_USE_SSL = settings["AWS_USE_SSL"]
        s3store.AWS_VERIFY = settings["AWS_VERIFY"]
        s3store.POLICY = settings["FILES_STORE_S3_ACL"]

        gcs_store: Type[GCSFilesStore] = cast(Type[GCSFilesStore], cls.STORE_SCHEMES["gs"])
        gcs_store.GCS_PROJECT_ID = settings["GCS_PROJECT_ID"]
        gcs_store.POLICY = settings["FILES_STORE_GCS_ACL"] or None

        ftp_store: Type[FTPFilesStore] = cast(Type[FTPFilesStore], cls.STORE_SCHEMES["ftp"])
        ftp_store.FTP_USERNAME = settings["FTP_USER"]
        ftp_store.FTP_PASSWORD = settings["FTP_PASSWORD"]
        ftp_store.USE_ACTIVE_MODE = settings.getbool("FEED_STORAGE_FTP_ACTIVE")

        return cls(settings=settings, crawler=crawler)

    def spider_opened(self) -> None:
        self.writer.write_warcinfo(robotstxt_obey=self.settings["ROBOTSTXT_OBEY"])

    def response_downloaded(self, response: Response, request: Request, spider: Spider) -> None:
        request.meta["WARC-Date"] = get_formatted_dt_string(format=WARC_DT_FORMAT)

        # Write response WARC record
        record = self.writer.write_response(response, request)
        self.stats.inc_value("webarchive/exporter/response_written", spider=spider)
        self.stats.inc_value(
            f"webarchive/exporter/writer_status_count/{record.http_headers.get_statuscode()}", 
            spider=spider,
        )

        # Write request WARC record
        self.writer.write_request(request, concurrent_to=record)
        self.stats.inc_value("webarchive/exporter/request_written", spider=spider)

    def spider_closed(self, spider: Spider) -> None:
        wacz_creator = WaczFileCreator(
            store=self.store,
            warc_fname=self.writer.warc_fname,
            wacz_fname=self.wacz_fname,
            collection_name=spider.name,
            title=self.settings["SW_WACZ_TITLE"],
            description=self.settings["SW_WACZ_DESCRIPTION"],
        )
        wacz_creator.create()


def get_archive_uri_template_dt_variables() -> dict:
    current_date = datetime.now()

    return {
        "year": current_date.strftime("%Y"),
        "month": current_date.strftime("%m"),
        "day": current_date.strftime("%d"),
        "timestamp": current_date.strftime("%Y%m%d%H%M%S"),
    }
