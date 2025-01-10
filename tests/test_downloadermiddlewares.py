from contextlib import contextmanager

from scrapy.http.request import Request
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler

from scrapy_webarchive.downloadermiddlewares import WaczMiddleware

from . import get_test_data_path


class BaseTestWaczMiddleware:
    def setup_method(self):
        self.crawler = get_crawler(Spider)
        self.spider = self.crawler._create_spider("quotes")

    def _get_wacz_source_url(self) -> str:
        """Return the WACZ source URL for this test. Override in subclasses."""

        raise NotImplementedError

    def _get_settings(self, **new_settings):
        settings = {
            "SW_WACZ_SOURCE_URI": self._get_wacz_source_url(),
            "SW_WACZ_CRAWL": True,
        }
        settings.update(new_settings)
        return Settings(settings)

    @contextmanager
    def _middleware(self, **new_settings):
        settings = self._get_settings(**new_settings)
        mw = WaczMiddleware(settings, self.crawler.stats)
        mw.spider_opened(self.spider)
        yield mw


class TestWaczMiddleware(BaseTestWaczMiddleware):
    def _get_wacz_source_url(self):
        return get_test_data_path("warc_1_1", "quotes.wacz").as_uri()

    def test_retrieve_from_wacz_record_not_found(self):
        request = Request("http://www.example.com/")
        with self._middleware() as mw:
            response = mw.process_request(request, self.spider)
            assert response
            assert response.status == 404

    def test_retrieve_from_wacz(self):
        request = Request("https://quotes.toscrape.com/tag/love/")
        with self._middleware() as mw:
            response = mw.process_request(request, self.spider)
            assert response
            assert response.status == 200


class TestWaczMiddlewareMultiWacz(BaseTestWaczMiddleware):
    def _get_wacz_source_url(self):
        wacz_1 = get_test_data_path("warc_1_1", "quotes.wacz").as_uri()
        wacz_2 = get_test_data_path("warc_1_1", "goodreads.wacz").as_uri()
        return f'{wacz_1},{wacz_2}'

    def test_retrieve_from_first_wacz(self):
        request = Request("https://quotes.toscrape.com/tag/love/")
        with self._middleware() as mw:
            response = mw.process_request(request, self.spider)
            assert response
            assert response.status == 200

    def test_retrieve_from_second_wacz(self):
        request = Request("https://www.goodreads.com/quotes")
        with self._middleware() as mw:
            response = mw.process_request(request, self.spider)
            assert response
            assert response.status == 200
