# Scrapy Webarchive

A Web Archive extension for Scrapy


# Installation

Add to your `settings.py` or your spider configuration.

```python
EXTENSIONS = {
    'scrapy_webarchive.extensions.WaczExporter': 543,
}

DOWNLOADER_MIDDLEWARES = {
    'scrapy_webarchive.downloadermiddlewares.WaczMiddleware': 543,
}

# year, month, day and timestamp are the supported template variables that you can use.
ARCHIVE_EXPORT_URI = 's3://scrapy-webarchive/{year}/{month}/{day}/'
```
