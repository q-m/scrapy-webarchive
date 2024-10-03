from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.pipelines import files
from typing_extensions import Self

from scrapy_webarchive.utils import warc_date
from scrapy_webarchive.wacz import WaczFileCreator
from scrapy_webarchive.warc import WarcFileWriter


class WaczExporter:
    """WACZ exporter extension that writes spider requests/responses as WACZ during a crawl job."""

    STORE_SCHEMES = {
        "": files.FSFilesStore,
        "file": files.FSFilesStore,
        "s3": files.S3FilesStore,
        "gs": files.GCSFilesStore,
        "ftp": files.FTPFilesStore,
    }

    def __init__(self, crawler: Crawler) -> None:
        self.crawler = crawler
        self.settings = crawler.settings

        if not self.settings["ARCHIVE"]:
            raise NotConfigured

        self.store = self._get_store()
        self.writer = WarcFileWriter(collection_name=crawler.spider.name)
        self.writer.write_warcinfo()

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        o = cls(crawler)
        crawler.signals.connect(o.response_received, signal=signals.response_received)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def response_received(self, response: Response, request: Request) -> None:
        # TODO: Move this
        request.meta["WARC-Date"] = warc_date()

        # Write response WARC record
        response_record_id = self.writer.write_response(response, request)

        # Write request WARC record
        self.writer.write_request(request, concurrent_to=response_record_id)

    def spider_closed(self) -> None:
        wacz_creator = WaczFileCreator(warc_fname=self.writer.warc_fname, store=self.store)
        wacz_creator.create_wacz()

    def _get_context_variables(self):
        current_date = datetime.now()

        return {
            "year": current_date.strftime("%Y"),
            "month": current_date.strftime("%m"),
            "day": current_date.strftime("%d"),
            "timestamp": current_date.strftime("%Y%m%d%H%M%S"),
        }

    def _get_store(self):
        archive_dir_template = self.settings["ARCHIVE"]["archive_dir"]
        uri = archive_dir_template.format(**self._get_context_variables())
    
        if Path(uri).is_absolute():  # to support win32 paths like: C:\\some\dir
            scheme = "file"
        else:
            scheme = urlparse(uri).scheme
        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(uri)
