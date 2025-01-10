from __future__ import annotations

import re
from urllib.parse import urlparse

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector
from typing_extensions import Iterable, Self, Union

from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.wacz.storages import ZipStorageHandlerFactory
from scrapy_webarchive.wacz.wacz_file import MultiWaczFile, WaczFile
from scrapy_webarchive.warc import record_transformer


class BaseWaczMiddleware:
    """A base class for middlewares that require opening one or more WACZ files."""

    wacz: Union[WaczFile, MultiWaczFile]

    def __init__(self, settings: Settings, stats: StatsCollector) -> None:
        self.stats = stats
        wacz_uri = settings.get("SW_WACZ_SOURCE_URI", None)

        if not wacz_uri:
            raise NotConfigured

        self.wacz_uris = re.split(r"\s*,\s*", wacz_uri)
        self.crawl = settings.get("SW_WACZ_CRAWL", False)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        assert crawler.stats
        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signals.spider_opened)
        return o

    def spider_opened(self, spider: Spider) -> None:
        """
        Handles the initialization of WACZ files when the spider is opened.

        This method is called when the Scrapy spider starts, and it attempts to open WACZ files 
        defined in the spider's configuration. It processes multiple WACZ URIs provided in `self.wacz_uris`, logs the 
        process, and collects valid WACZ files for further use.

        If only one WACZ URI is provided, it opens and assigns the file to `self.wacz` as a `WaczFile` instance. 
        If multiple URIs are provided, valid files are assigned to `self.wacz` as a `MultiWaczFile` instance.
        """

        spider.logger.info(f"[WACZDownloader] Found {len(self.wacz_uris)} WACZ URI(s) to open")
        wacz_files = []
        
        for wacz_uri in self.wacz_uris:
            spider.logger.info(f"[WACZDownloader] Opening WACZ {wacz_uri}")
            storage_handler = ZipStorageHandlerFactory.get_handler(wacz_uri, spider.settings)

            if not storage_handler.zip_exists:
                spider.logger.error(f"[WACZDownloader] Could not open WACZ {wacz_uri}")
                continue

            wacz_files.append(WaczFile(storage_handler=storage_handler))

        if wacz_files:
            spider.logger.info(
                f"[WACZDownloader] Continuing with {len(wacz_files)}/{len(self.wacz_uris)} valid WACZ files"
            )
            self.wacz = wacz_files[0] if len(wacz_files) == 1 else MultiWaczFile(wacz_files)

        # If there are not wacz_files, we raise a `WaczMiddlewareException` in the downloader/spider middleware.
        # Raising an exception here does not stop the job from running. If there are no valid WACZ files configured
        # we do not want to continue the job.

    def _is_off_site(self, url: str, spider: Spider) -> bool:
        """Check if the URL is off-site based on allowed domains."""

        return hasattr(spider, "allowed_domains") and urlparse(url).hostname not in spider.allowed_domains

    def _is_disallowed_by_spider(self, url: str, spider: Spider) -> bool:
        """Check if the URL is disallowed by the spider's archive rules."""

        return hasattr(spider, "archive_disallow_regexp") and not re.search(spider.archive_disallow_regexp, url)


class WaczCrawlMiddleware(BaseWaczMiddleware):
    """
    Scrapy WACZ crawl spider middleware to crawl from a WACZ archive.

    Replaces the default behaviour of the spider by solely iterating over the
    entries in the WACZ file.
    """

    def spider_opened(self, spider: Spider) -> None:
        if not self.crawl:
            return

        super().spider_opened(spider)

    def process_start_requests(self, start_requests: Iterable[Request], spider: Spider) -> Iterable[Request]:
        """Processes start requests and yields WACZ index entries or original requests based on the crawl setting."""
        
        # If crawl is disabled, yield the original start requests.
        if not self.crawl:
            yield from start_requests
            return
        # If the attribute has not been set, none of the WACZ could be opened.
        elif not hasattr(self, "wacz"):
            raise WaczMiddlewareException("Could not open any WACZ files, check your WACZ URIs and authentication.")

        # Iterate over entries in the WACZ index.
        for entry in self.wacz.iter_index():
            url = entry.data["url"]

            # Filter out off-site requests
            if self._is_off_site(url, spider):
                self.stats.inc_value("webarchive/crawl_skip/off_site", spider=spider)
                flags = ["wacz_start_request", "wacz_crawl_skip"]
            # Ignore disallowed pages (to avoid crawling e.g. redirects from whitelisted pages to unwanted ones)
            elif self._is_disallowed_by_spider(url, spider):
                self.stats.inc_value("webarchive/crawl_skip/disallowed", spider=spider)
                flags = ["wacz_start_request", "wacz_crawl_skip"]
            else:
                self.stats.inc_value("webarchive/start_request_count", spider=spider)
                flags = ["wacz_start_request"]

            yield record_transformer.request_for_record(
                entry,
                flags=flags,
                meta={"cdxj_record": entry},
                dont_filter=True,
            )
