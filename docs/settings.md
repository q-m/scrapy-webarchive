# Settings

`scrapy-webarchive` makes use of the following settings, in addition to Scrapy's settings:

## Extensions

### `ARCHIVE_EXPORT_URI`

```python
ARCHIVE_EXPORT_URI = "s3://scrapy-webarchive/"
ARCHIVE_EXPORT_URI = "s3://scrapy-webarchive/{year}/{month}/{day}/"
```

This is the output path of the WACZ file. Multiple variables can be added that allow dynamic generation of the output path. 

Supported variables: `year`, `month`, `day` and `timestamp`.

## Downloader middleware

### `WACZ_SOURCE_URL`

```python
WACZ_SOURCE_URL = "s3://scrapy-webarchive/archive.wacz"
```

This setting defines the location of the WACZ file that should be used as a source for the crawl job.

### `WACZ_CRAWL`

```python
WACZ_CRAWL = True
```

Setting to ignore original `start_requests`, just yield all responses found.
