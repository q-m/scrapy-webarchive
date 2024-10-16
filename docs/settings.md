# Settings

`scrapy-webarchive` makes use of the following settings, in addition to Scrapy's settings. Note that all the settings are prefixed with `SW_`.

## Extensions

### `SW_EXPORT_URI`

```python
SW_EXPORT_URI = "s3://scrapy-webarchive/"
SW_EXPORT_URI = "s3://scrapy-webarchive/{year}/{month}/{day}/"
```

This is the output path of the WACZ file. Multiple variables can be added that allow dynamic generation of the output path. 

Supported variables: `year`, `month`, `day` and `timestamp`.

## Downloader middleware and spider middleware

### `SW_WACZ_SOURCE_URI`

```python
SW_WACZ_SOURCE_URI = "s3://scrapy-webarchive/archive.wacz"

# Allows multiple sources, comma seperated.
SW_WACZ_SOURCE_URI = "s3://scrapy-webarchive/archive.wacz,/path/to/archive.wacz"
```

This setting defines the location of the WACZ file that should be used as a source for the crawl job.

### `SW_WACZ_CRAWL`

```python
SW_WACZ_CRAWL = True
```

Setting to ignore original `start_requests`, just yield all responses found.

### `SW_WACZ_TIMEOUT`

```python
SW_WACZ_TIMEOUT = 60
```

Transport parameter for retrieving the `SW_WACZ_SOURCE_URI` from the defined location.
