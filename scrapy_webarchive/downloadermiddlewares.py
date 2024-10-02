import re
from typing import Iterable
from urllib.parse import urlparse

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import IgnoreRequest, NotConfigured
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.responsetypes import ResponseTypes
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.statscollectors import StatsCollector
from smart_open import open
from typing_extensions import Self
from warc.warc import WARCRecord

from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.utils import MultiWACZFile, WACZFile


class WaczMiddleware:
    """
    Scrapy downloader middleware to crawl from a WACZ archive

    Loads the index fully into memory, but lazily loads pages.
    This helps to work with large archives, including remote ones.
    """

    response_types = ResponseTypes()

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

        if len(self.wacz_urls) == 1:
            spider.logger.info(f"[WACZDownloader] Opening WACZ {self.wacz_urls[0]}")
            self.wacz = WACZFile(open(self.wacz_urls[0], "rb", transport_params=tp))
        else:
            spider.logger.info(f"[WACZDownloader] Opening WACZs {self.wacz_urls}")
            self.wacz = MultiWACZFile(
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
                yield self._request_for_record(
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
            response = self._response_for_record(record)

            if not response:
                self.stats.inc_value("wacz/response_not_recognized", spider=spider)
                raise IgnoreRequest()

            self.stats.inc_value("wacz/hit", spider=spider)
            return response
        else:
            # when page not found in archive, return 404, and record it in a statistic
            self.stats.inc_value("wacz/response_not_found", spider=spider)
            return Response(url=request.url, status=404)

    def _request_for_record(self, record: WARCRecord, **kwargs):
        # TODO locate request in WACZ and include all relevant things (like headers)
        return Request(url=record["url"], method=record.get("method", "GET"), **kwargs)

    def _response_for_record(self, record: WARCRecord, **kwargs):
        # We expect a response.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-type-mandatory
        if record["WARC-Type"] != "response":
            raise WaczMiddlewareException(f"Unexpected record type: {record['type']}")

        # We only know how to handle application/http.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        record_content_type = (record["Content-Type"] or "").split(";", 1)[0]
        if record_content_type != "application/http":
            raise WaczMiddlewareException(
                f"Unexpected record content-type: {record_content_type}"
            )

        # There is a date field in record['WARC-Date'], but don't have a use for it now.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-date-mandatory

        payload = record.payload.read()
        payload_parts = payload.split(b"\r\n\r\n", 1)
        header_lines = payload_parts[0] if len(payload_parts) > 0 else ""
        body = payload_parts[1] if len(payload_parts) > 1 else None

        header_lines = header_lines.split(b"\r\n")
        header_parts = header_lines[0].split(None, 2)
        protocol = header_parts[0] if len(header_parts) > 0 else None
        status = header_parts[1] if len(header_parts) > 1 else None
        headers = header_lines_to_dict(header_lines[1:])

        if not status or not protocol:
            return None

        response_cls = self.response_types.from_headers(headers)

        return response_cls(
            url=record["WARC-Target-URI"],
            status=int(status.decode()),
            protocol=protocol.decode(),
            headers=headers,
            body=body,
            **kwargs,
        )


def header_lines_to_dict(lines):
    # XXX only supports each header appearing once, not multiple occurences
    headers = {}
    for line in lines:
        k, v = line.split(b":", 1)
        v = v.lstrip()
        headers[k] = v
    return headers
