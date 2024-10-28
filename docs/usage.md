# Usage

The general use for this plugin is separated in two parts, exporting and crawling.

1. **Exporting**; Run your spider with the extension to generate and export a WACZ file. This WACZ archive can be used in future crawls to retrieve historical data or simply to decrease the load on the website when your spider has changed but needs to run on the same data.
2. **Crawling**; Re-run your spider on an WACZ archive that was generated previously. This time we will not be generating a new WACZ archive but simply retrieve each reponse from the WACZ instead of making a request to the live resource (website). The WACZ contains complete response data that will be reconstructed to actual `Response` objects.

## Exporting

### Exporting a WACZ archive

To archive the requests/responses during a crawl job you need to enable the `WaczExporter` extension. 

```python
EXTENSIONS = {
    "scrapy_webarchive.extensions.WaczExporter": 543,
}
```

This extension also requires you to set the export location using the `SW_EXPORT_URI` settings (check the settings page for different options for exporting).

```python
SW_EXPORT_URI = "s3://scrapy-webarchive/"
```

Running a crawl job using these settings will result in a newly created WACZ file on the specified output location.

## Crawling

### Using the download middleware

To crawl against a WACZ archive you need to use the `WaczMiddleware` downloader middleware. Instead of fetching the live resource the middleware will retrieve it from the archive and recreate a `Response` using the data from the archive.

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
