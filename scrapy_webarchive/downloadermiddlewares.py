import re
from typing import Iterable, Union
from urllib.parse import urlparse

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

    def __init__(self, settings: Settings, stats: StatsCollector) -> None:
        self.stats = stats
        wacz_url = settings.get("WACZ_SOURCE_URL", None)

        if not wacz_url:
            raise NotConfigured

        self.wacz_urls = re.split(r"\s*,\s*", wacz_url)
        self.crawl = settings.get("WACZ_CRAWL", False)
        self.timeout = settings.getfloat("WACZ_TIMEOUT", 60)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        assert crawler.stats
        o = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(o.spider_opened, signals.spider_opened)
        return o

    def spider_opened(self, spider: Spider) -> None:
        tp = {"timeout": self.timeout}
        self.wacz: Union[WaczFile, MultiWaczFile]

        if len(self.wacz_urls) == 1:
            spider.logger.info(f"[WACZDownloader] Opening WACZ {self.wacz_urls[0]}")
            self.wacz = WaczFile(open(self.wacz_urls[0], "rb", transport_params=tp))
        else:
            spider.logger.info(f"[WACZDownloader] Opening WACZs {self.wacz_urls}")
            self.wacz = MultiWaczFile(
                [open(u, "rb", transport_params=tp) for u in self.wacz_urls]
            )

    def process_start_requests(self, start_requests: Iterable[Request], spider: Spider):
        if not self.crawl:
            for request in start_requests:
                yield request

        if self.crawl:
            # ignore original start requests, just yield all responses found
            for entry in self.wacz.iter_index():
                url = entry["url"]

                # filter out off-site responses
                if urlparse(url).hostname not in spider.allowed_domains:
                    continue

                # only accept whitelisted responses if requested by spider
                if hasattr(spider, "archive_regexp") and not re.search(spider.archive_regexp, url):
                    continue

                self.stats.inc_value("wacz/start_request_count", spider=spider)

                # do not filter to allow all occurences to be handled
                # since we don't yet get all information for the request, this can be necessary
                yield record_transformer.request_for_record(
                    entry,
                    flags=["wacz_start_request"],
                    meta={"wacz_index_entry": entry},
                    dont_filter=True,
                )

    def process_request(self, request: Request, spider: Spider):
        # ignore blacklisted pages (to avoid crawling e.g. redirects from whitelisted pages to unwanted ones)
        if hasattr(spider, "archive_blacklist_regexp") and re.search(
            spider.archive_blacklist_regexp, request.url
        ):
            self.stats.inc_value("wacz/request_blacklisted", spider=spider)
            raise IgnoreRequest()

        # ignore when crawling and flag indicates this request needs to be skipped during wacz crawl
        if self.crawl and "wacz_crawl_skip" in request.flags:
            self.stats.inc_value("wacz/crawl_skip", spider=spider)
            raise IgnoreRequest()

        # get record from existing index entry, or else lookup by URL
        record = self.wacz.get_record(request.meta.get("wacz_index_entry", request.url))
        if record:
            response = record_transformer.response_for_record(record)

            if not response:
                self.stats.inc_value("wacz/response_not_recognized", spider=spider)
                raise IgnoreRequest()

            self.stats.inc_value("wacz/hit", spider=spider)
            return response
        else:
            # when page not found in archive, return 404, and record it in a statistic
            self.stats.inc_value("wacz/response_not_found", spider=spider)
            return Response(url=request.url, status=404)
