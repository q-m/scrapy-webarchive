# Changelog

## [x.x.x] - In development

### Changed

- Make `WARC/1.1` the default in the `WarcFileWriter` ([#28](https://github.com/q-m/scrapy-webarchive/pull/28)) ([`3569600`](https://github.com/q-m/scrapy-webarchive/commit/3569600))

### Added

- Introduced special meta key for WARC metadata during crawling/exporting ([#31](https://github.com/q-m/scrapy-webarchive/pull/31)) ([`f836f02`](https://github.com/q-m/scrapy-webarchive/commit/f836f02))
- Added example spider for testing purposes
- Document differences between URIs


## [0.3.0] - 2025-01-10

### Changed

- Change `_check_configuration_prerequisites` logic in `WaczExporter`


## [0.2.0] - 2025-01-10

### Changed

- Adjusted logic to always use downloader middleware when `SW_WACZ_SOURCE_URI` is configured

### Removed

- Removed the unused `smart-open` dependency


## [0.1.0] - 2025-01-10

:seedling: Initial release.
