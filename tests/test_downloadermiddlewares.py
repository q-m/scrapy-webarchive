from contextlib import contextmanager

from scrapy.http.request import Request
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler

from scrapy_webarchive.downloadermiddlewares import WaczMiddleware

from . import get_test_data_path


class TestWaczMiddleware:
    def setup_method(self):
        self.crawler = get_crawler(Spider)
        self.spider = self.crawler._create_spider("quotes")

    def _get_settings(self, **new_settings):
        settings = {
            "SW_WACZ_SOURCE_URL": get_test_data_path("warc_1_1", "quotes.wacz.gz").as_uri(),
            "SW_WACZ_CRAWL": False,
            "SW_WACZ_TIMEOUT": 60,
        }
        settings.update(new_settings)
        return Settings(settings)
    
    @contextmanager
    def _middleware(self, **new_settings):
        settings = self._get_settings(**new_settings)
        mw = WaczMiddleware(settings, self.crawler.stats)
        mw.spider_opened(self.spider)
        yield mw

    def test_retrieve_from_wacz(self):
        # Response for the URL exists in the WACZ archive.
        request = Request("https://quotes.toscrape.com/tag/love/")

        with self._middleware() as mw:
            response = mw.process_request(request, self.spider)
            assert response
            assert response.status == 200

    def test_retrieve_from_wacz_record_not_found(self):
        request = Request("https://example.com/")

        with self._middleware() as mw:
            response = mw.process_request(request, self.spider)
            assert response
            assert response.status == 404
