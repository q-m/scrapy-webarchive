from __future__ import annotations

from scrapy.exceptions import IgnoreRequest
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.spiders import Spider

from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.spidermiddlewares import BaseWaczMiddleware
from scrapy_webarchive.warc import record_transformer


class WaczMiddleware(BaseWaczMiddleware):
    """
    Scrapy downloader middleware to crawl from a WACZ archive.

    Loads the index fully into memory, but lazily loads pages.
    This helps to work with large archives, including remote ones.
    """

    def _check_ignore_conditions(self, request: Request, spider: Spider) -> None:
        """Check conditions that would lead to ignoring the request and raise IgnoreRequest if necessary."""

        # Ignore when crawling and flag indicates this request needs to be skipped during WACZ crawl.
        if "wacz_crawl_skip" in request.flags:
            self.stats.inc_value("webarchive/crawl_skip", spider=spider)
            raise IgnoreRequest()

        # Filter out off-site requests.
        if self._is_off_site(request.url, spider):
            self.stats.inc_value("webarchive/crawl_skip/off_site", spider=spider)
            raise IgnoreRequest()

        # Ignore disallowed pages (to avoid crawling e.g. redirects from whitelisted pages to unwanted ones).
        if self._is_disallowed_by_spider(request.url, spider):
            self.stats.inc_value("webarchive/crawl_skip/disallowed", spider=spider)
            raise IgnoreRequest()

    def process_request(self, request: Request, spider: Spider):
        """Called for each request that goes through the downloader."""

        # If the attribute has not been set, none of the WACZ could be opened.
        if not hasattr(self, "wacz"):
            raise WaczMiddlewareException("Could not open any WACZ files, check your WACZ URIs and authentication.")

        # Check if the request should be ignored.
        self._check_ignore_conditions(request, spider)

        # Get record from existing index entry, or else lookup by URL.
        if request.meta.get("cdxj_record"):
            warc_record = self.wacz.get_warc_from_cdxj_record(cdxj_record=request.meta["cdxj_record"])
        else:
            warc_record = self.wacz.get_warc_from_url(url=request.url)

        # When page not found in archive, return status 404 and record it in a statistic.
        if not warc_record:
            self.stats.inc_value("webarchive/response_not_found", spider=spider)
            return Response(url=request.url, status=404)
        
        # Record found, try to re-create a response from it.
        response = record_transformer.response_for_record(warc_record)

        if not response:
            self.stats.inc_value("webarchive/response_not_recognized", spider=spider)
            raise IgnoreRequest()

        self.stats.inc_value("webarchive/hit", spider=spider)
        return response            
