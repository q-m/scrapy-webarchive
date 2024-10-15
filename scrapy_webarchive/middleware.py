import re
from typing import IO, List, Union
from urllib.parse import urlparse

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector
from smart_open import open
from typing_extensions import Iterable, Self

from scrapy_webarchive.wacz import MultiWaczFile, WaczFile
from scrapy_webarchive.warc import record_transformer


class BaseWaczMiddleware:
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

        def open_wacz_file(wacz_url: str) -> Union[IO, None]:
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
            wacz_files: List[IO] = []

            for wacz_url in self.wacz_urls:
                wacz_file = open_wacz_file(wacz_url)
                if wacz_file:
                    wacz_files.append(wacz_file)
    
            if wacz_files:
                self.wacz = MultiWaczFile(wacz_files)


class WaczCrawlMiddleware(BaseWaczMiddleware):
    def spider_opened(self, spider: Spider) -> None:
        if not self.crawl:
            return

        super().spider_opened(spider)

    def process_start_requests(self, start_requests: Iterable[Request], spider: Spider):
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

                self.stats.inc_value("wacz/start_request_count", spider=spider)

                # do not filter to allow all occurences to be handled
                # since we don't yet get all information for the request, this can be necessary
                yield record_transformer.request_for_record(
                    entry,
                    flags=["wacz_start_request"],
                    meta={"cdxj_record": entry},
                    dont_filter=True,
                )
