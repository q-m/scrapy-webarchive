# Changelog

## [next] - xxxx-xx-xx

## [0.5.1] - 2026-03-12

- Fix mismatch between crawling from local storage vs. S3

## [0.5.0] - 2026-03-11

- Add `archive_regexp`, `archive_blacklist_regexp`; remove `archive_disallow_regexp` ([#39](https://github.com/q-m/scrapy-webarchive/issues/39)) - _possibly breaking change_
- Ignore unrecognized index entries ([#38](https://github.com/q-m/scrapy-webarchive/issues/38))
- Fix reading compressed index files ([#42](https://github.com/q-m/scrapy-webarchive/issues/42))

## [0.4.1] - 2025-11-17

- Fix for getting spider name in different scrapy versions

## [0.4.0] - 2025-02-28

### Changed

- Make `WARC/1.1` the default in the `WarcFileWriter` ([#28](https://github.com/q-m/scrapy-webarchive/pull/28)) ([`3569600`](https://github.com/q-m/scrapy-webarchive/commit/3569600))

### Added

- Introduced special meta key for WARC metadata during crawling/exporting ([#31](https://github.com/q-m/scrapy-webarchive/pull/31)) ([`f836f02`](https://github.com/q-m/scrapy-webarchive/commit/f836f02))
- Added example spider for testing purposes
- Document differences between URIs
- Settings to run a spider against previously generated archives ([#32](https://github.com/q-m/scrapy-webarchive/pull/31)) ([`f28e966`](https://github.com/q-m/scrapy-webarchive/commit/f28e966))


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
