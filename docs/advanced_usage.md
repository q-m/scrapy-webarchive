# Advanced usage

## Crawling

### Iterating a WACZ archive index

Going around the default behaviour of the spider, the `WaczCrawlMiddleware` spider middleware will, when enabled, replace the crawl by an iteration through all the entries in the WACZ archive index.

To use this strategy, enable both middlewares in the spider settings like so:

```python
DOWNLOADER_MIDDLEWARES = {
    "scrapy_webarchive.downloadermiddlewares.WaczMiddleware": 543,
}

SPIDER_MIDDLEWARES = {
    "scrapy_webarchive.spidermiddlewares.WaczCrawlMiddleware": 543,
}
```

Then define the location of the WACZ archive with `SW_WACZ_SOURCE_URI` setting:

```python
SW_WACZ_SOURCE_URI = "s3://scrapy-webarchive/archive.wacz"
SW_WACZ_CRAWL = True
```
