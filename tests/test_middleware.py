from contextlib import contextmanager

from scrapy.http.request import Request
from scrapy.settings import Settings
from scrapy.utils.test import get_crawler

from scrapy_webarchive.spidermiddlewares import WaczCrawlMiddleware

from . import get_test_data_path


class TestWaczCrawlMiddlewareWarc11:
    def setup_method(self):
        self.crawler = get_crawler()
        self.spider = self.crawler._create_spider("quotes")

    def _get_settings(self, **new_settings):
        settings = {
            "SW_WACZ_SOURCE_URI": get_test_data_path("warc_1_1", "quotes.wacz").as_uri(),
        }
        settings.update(new_settings)
        return Settings(settings)
    
    @contextmanager
    def _middleware(self, **new_settings):
        settings = self._get_settings(**new_settings)
        mw = WaczCrawlMiddleware(settings, self.crawler.stats)
        mw.spider_opened(self.spider)
        yield mw

    def test_wacz_archive_is_ignored_follow_original_behaviour(self):
        request = Request("https://quotes.toscrape.com")

        with self._middleware(SW_WACZ_CRAWL=False) as mw:
            out = list(mw.process_start_requests([request], self.spider))
            assert out == [request]

    def test_wacz_archive_iterates_all_records(self):
        with self._middleware(SW_WACZ_CRAWL=True) as mw:
            out = list(mw.process_start_requests([], self.spider))
            assert len(out) == 101

    def test_wacz_archive_filters_allowed_domains(self):
        setattr(self.spider, "allowed_domains", "quotes.toscrape.com")

        with self._middleware(SW_WACZ_CRAWL=True) as mw:
            out = list(mw.process_start_requests([], self.spider))
            assert len([request for request in out if "wacz_crawl_skip" not in request.flags]) == 61

    def test_wacz_archive_filters_archive_regex(self):
        setattr(self.spider, "archive_disallow_regexp", r"https://quotes\.toscrape\.com/page/\d+/")

        with self._middleware(SW_WACZ_CRAWL=True) as mw:
            out = list(mw.process_start_requests([], self.spider))
            assert len([request for request in out if "wacz_crawl_skip" not in request.flags]) == 9
