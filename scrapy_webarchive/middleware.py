import re
from typing import IO, List, Union
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
        multiple_entries = len(self.wacz_uris) != 1

        if not multiple_entries:
            wacz_uri = self.wacz_uris[0]
            spider.logger.info(f"[WACZDownloader] Opening WACZ {wacz_uri}")
            wacz_file = open_wacz_file(wacz_uri, self.timeout, spider.settings)
            if wacz_file:
                self.wacz = WaczFile(wacz_file)
            else:
                spider.logger.error(f"[WACZDownloader] Could not open WACZ {wacz_uri}")
        else:
            wacz_files: List[IO] = []

            for wacz_uri in self.wacz_uris:
                spider.logger.info(f"[WACZDownloader] Opening WACZ {wacz_uri}")
                wacz_file = open_wacz_file(wacz_uri, self.timeout, spider.settings)
                if wacz_file:
                    wacz_files.append(wacz_file)
                else:
                    spider.logger.error(f"[WACZDownloader] Could not open WACZ {wacz_uri}")
    
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
