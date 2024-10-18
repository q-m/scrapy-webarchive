import re
from typing import Union
from urllib.parse import urlparse

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector
from typing_extensions import Iterable, Self

from scrapy_webarchive.wacz import MultiWaczFile, WaczFile, open_wacz_file
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
        self.timeout = settings.getfloat("SW_WACZ_TIMEOUT", 60.0)

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
        If multiple URIs are provided, valid files are grouped and assigned to `self.wacz` as a `MultiWaczFile` instance. 
        """

        spider.logger.info(f"[WACZDownloader] Found {len(self.wacz_uris)} WACZ URI(s) to open")
        wacz_files = []
        
        for wacz_uri in self.wacz_uris:
            spider.logger.info(f"[WACZDownloader] Opening WACZ {wacz_uri}")
            wacz_file = open_wacz_file(wacz_uri, self.timeout, spider.settings)
            if wacz_file:
                wacz_files.append(wacz_file)
            else:
                spider.logger.error(f"[WACZDownloader] Could not open WACZ {wacz_uri}")

        if wacz_files:
            spider.logger.info(f"[WACZDownloader] Continuing with {len(wacz_files)}/{len(self.wacz_uris)} valid WACZ files")
            if len(wacz_files) == 1:
                self.wacz = WaczFile(wacz_files[0])
            else:
                self.wacz = MultiWaczFile(wacz_files)


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
        if not self.crawl or not hasattr(self, 'wacz'):
            for request in start_requests:
                yield request

        # Ignore original start requests, just yield all responses found
        else:
            for entry in self.wacz.iter_index():
                url = entry.data["url"]

                # filter out off-site responses
                if hasattr(spider, "allowed_domains") and urlparse(url).hostname not in spider.allowed_domains:
                    continue

                # only accept allowed responses if requested by spider
                if hasattr(spider, "archive_regex") and not re.search(spider.archive_regex, url):
                    continue

                self.stats.inc_value("webarchive/start_request_count", spider=spider)

                # do not filter to allow all occurences to be handled
                # since we don't yet get all information for the request, this can be necessary
                yield record_transformer.request_for_record(
                    entry,
                    flags=["wacz_start_request"],
                    meta={"cdxj_record": entry},
                    dont_filter=True,
                )
