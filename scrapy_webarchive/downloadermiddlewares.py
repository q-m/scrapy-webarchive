import re

from scrapy.exceptions import IgnoreRequest
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.spiders import Spider

from scrapy_webarchive.middleware import BaseWaczMiddleware
from scrapy_webarchive.warc import record_transformer


class WaczMiddleware(BaseWaczMiddleware):
    """
    Scrapy downloader middleware to crawl from a WACZ archive

    Loads the index fully into memory, but lazily loads pages.
    This helps to work with large archives, including remote ones.
    """

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
