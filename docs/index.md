# Scrapy Webarchive

[![Docs](https://github.com/q-m/scrapy-webarchive/actions/workflows/docs.yml/badge.svg)](https://github.com/q-m/scrapy-webarchive/actions/workflows/docs.yml)

`scrapy-webarchive` is a plugin for Scrapy that allows users to capture and export web archives in the WARC and WACZ formats during crawling.

## Features
- Save web crawls in WACZ format (multiple storages supported; local and cloud).
- Crawl against WACZ format archives.
- Integrate seamlessly with Scrapy’s spider request and response cycle.

## Limitations
- WACZ supports saving images but this module does not yet integrate with Scrapy's image/file pipeline for retrieving images/files from the WACZ. Future support for this feature is planned.

**Source Code**: <a href="https://github.com/q-m/scrapy-webarchive" target="_blank">https://github.com/q-m/scrapy-webarchive</a>

## Credits

This package started as a fork of https://github.com/internetarchive/scrapy-warcio. The idea of turning its functionality into an extension as well as actually writing the WARC files is based on it.
