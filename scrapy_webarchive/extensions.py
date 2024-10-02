from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.http.request import Request
from scrapy.http.response import Response
from typing_extensions import Self

from scrapy_webarchive.utils import warc_date
from scrapy_webarchive.warcio import ScrapyWarcIo


class WaczExporter:
    """WACZ exporter extension that writes spider requests/responses as WACZ during a crawl job."""

    def __init__(self, crawler: Crawler) -> None:
        self.warcio = ScrapyWarcIo(collection_name=crawler.spider.name)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        o = cls(crawler)
        crawler.signals.connect(o.response_received, signal=signals.response_received)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def response_received(self, response: Response, request: Request) -> None:
        request.meta["WARC-Date"] = warc_date()
        self.warcio.write(response, request)

    def spider_closed(self) -> None:
        self.warcio.create_wacz()
