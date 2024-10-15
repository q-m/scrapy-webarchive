import re
from typing import IO, List, Union

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import IgnoreRequest, NotConfigured
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.statscollectors import StatsCollector
from smart_open import open
from typing_extensions import Self

from scrapy_webarchive.wacz import MultiWaczFile, WaczFile
from scrapy_webarchive.warc import record_transformer


class WaczMiddleware:
    """
    Scrapy downloader middleware to crawl from a WACZ archive

    Loads the index fully into memory, but lazily loads pages.
    This helps to work with large archives, including remote ones.
    """
    
    wacz: Union[WaczFile, MultiWaczFile]

    def __init__(self, settings: Settings, stats: StatsCollector) -> None:
        self.stats = stats
        wacz_url = settings.get("SW_WACZ_SOURCE_URL", None)

        if not wacz_url:
            raise NotConfigured

        self.wacz_urls = re.split(r"\s*,\s*", wacz_url)
        self.crawl = settings.get("SW_WACZ_CRAWL", False)
        self.timeout = settings.getfloat("SW_WACZ_TIMEOUT", 60)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        assert crawler.stats
        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signals.spider_opened)
        return o

    def spider_opened(self, spider: Spider) -> None:
        tp = {"timeout": self.timeout}
        multiple_entries = len(self.wacz_urls) != 1

        def open_wacz_file(wacz_url: str) -> Union[IO[bytes], None]:
            spider.logger.info(f"[WACZDownloader] Opening WACZ {wacz_url}")
            
            try:
                return open(wacz_url, "rb", transport_params=tp)
            except OSError:
                spider.logger.error(f"[WACZDownloader] Could not open WACZ {wacz_url}")
                return None

        if not multiple_entries:
            wacz_url = self.wacz_urls[0]
            wacz_file = open_wacz_file(wacz_url)
            if wacz_file:
                self.wacz = WaczFile(wacz_file)
        else:
            wacz_files: List[IO[bytes]] = []

            for wacz_url in self.wacz_urls:
                wacz_file = open_wacz_file(wacz_url)
                if wacz_file:
                    wacz_files.append(wacz_file)
    
            if wacz_files:
                self.wacz = MultiWaczFile(wacz_files)

    def process_request(self, request: Request, spider: Spider):
        if not hasattr(self, 'wacz'):
            self.stats.set_value("wacz/no_valid_sources", True, spider=spider)
            raise IgnoreRequest()

        # ignore blacklisted pages (to avoid crawling e.g. redirects from whitelisted pages to unwanted ones)
        if hasattr(spider, "archive_blacklist_regexp") and re.search(spider.archive_blacklist_regexp, request.url):
            self.stats.inc_value("wacz/request_blacklisted", spider=spider)
            raise IgnoreRequest()

        # ignore when crawling and flag indicates this request needs to be skipped during wacz crawl
        if self.crawl and "wacz_crawl_skip" in request.flags:
            self.stats.inc_value("wacz/crawl_skip", spider=spider)
            raise IgnoreRequest()

        # get record from existing index entry, or else lookup by URL
        if request.meta.get("cdxj_record"):
            warc_record = self.wacz.get_warc_from_cdxj_record(cdxj_record=request.meta["cdxj_record"])
        else:
            warc_record = self.wacz.get_warc_from_url(url=request.url)

        # When page not found in archive, return 404, and record it in a statistic
        if not warc_record:
            self.stats.inc_value("wacz/response_not_found", spider=spider)
            return Response(url=request.url, status=404)
        
        # Record found
        response = record_transformer.response_for_record(warc_record)

        if not response:
            self.stats.inc_value("wacz/response_not_recognized", spider=spider)
            raise IgnoreRequest()

        self.stats.inc_value("wacz/hit", spider=spider)
        return response            
