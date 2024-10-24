# Usage

## Exporting

### Exporting a WACZ archive

To archive the requests/responses during a crawl job you need to enable the `WaczExporter` extension. 

```python
EXTENSIONS = {
    "scrapy_webarchive.extensions.WaczExporter": 543,
}
```

This extension also requires you to set the export location using the `SW_EXPORT_URI` settings.

```python
SW_EXPORT_URI = "s3://scrapy-webarchive/"
```

Running a crawl job using these settings will result in a newly created WACZ file.

## Crawling

There are 2 ways to crawl against a WACZ archive. Choose a strategy that you want to use for your crawl job, and follow the instruction as described below.

### Lookup in a WACZ archive

One of the ways to crawl against a WACZ archive is to use the `WaczMiddleware` downloader middleware. Instead of fetching the live resource the middleware will instead retrieve it from the archive and recreate a response using the data from the archive.

To use the downloader middleware, enable it in the settings like so:

```python
DOWNLOADER_MIDDLEWARES = {
    "scrapy_webarchive.downloadermiddlewares.WaczMiddleware": 543,
}
```

Then define the location of the WACZ archive with `SW_WACZ_SOURCE_URI` setting:

```python
SW_WACZ_SOURCE_URI = "s3://scrapy-webarchive/archive.wacz"
SW_WACZ_CRAWL = True
```

### Iterating a WACZ archive

Going around the default behaviour of the spider, the `WaczCrawlMiddleware` spider middleware will, when enabled, replace the crawl by an iteration through all the entries in the WACZ archive index. Then, similar to the previous strategy, it will recreate a response using the data from the archive.

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
