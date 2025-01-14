BOT_NAME = "webarchive_example"

SPIDER_MODULES = ["webarchive_example.spiders"]
NEWSPIDER_MODULE = "webarchive_example.spiders"

ROBOTSTXT_OBEY = True

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
DOWNLOAD_DELAY = 2.5

EXTENSIONS = {
    "scrapy_webarchive.extensions.WaczExporter": 543,
}

DOWNLOADER_MIDDLEWARES = {
    "scrapy_webarchive.downloadermiddlewares.WaczMiddleware": 543,
}

SPIDER_MIDDLEWARES = {
    "scrapy_webarchive.spidermiddlewares.WaczCrawlMiddleware": 543,
}
