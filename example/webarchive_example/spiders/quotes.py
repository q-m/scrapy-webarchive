from pathlib import Path

import scrapy


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    start_urls = ['https://quotes.toscrape.com/']
    custom_settings = {
        # 'SW_WACZ_SOURCE_URI': 'file:///path/to/wacz/quotes.wacz',
        'SW_EXPORT_URI': str(Path(__file__).resolve().parent),
        'CLOSESPIDER_ITEMCOUNT': 5,
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, dont_filter=True)

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').extract_first(),
                'author': quote.css('small.author::text').extract_first(),
                'tags': quote.css('div.tags > a.tag::text').extract()
            }

        next_page_url = response.css('li.next > a::attr(href)').extract_first()
        if next_page_url is not None:
            yield scrapy.Request(response.urljoin(next_page_url))
