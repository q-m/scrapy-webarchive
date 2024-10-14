import re
from urllib.parse import urlparse

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector
from smart_open import open
from typing_extensions import Iterable, Self, Union

from scrapy_webarchive.wacz import MultiWaczFile, WaczFile
from scrapy_webarchive.warc import record_transformer


class WaczCrawlMiddleware:
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
        if not self.crawl:
            return

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
        else:  # ignore original start requests, just yield all responses found
            for entry in self.wacz.iter_index():
                url = entry["url"]

                # filter out off-site responses
                if hasattr(spider, "allowed_domains") and urlparse(url).hostname not in spider.allowed_domains:
                    continue

                # only accept allowed responses if requested by spider
                if hasattr(spider, "archive_regex") and not re.search(spider.archive_regex, url):
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
