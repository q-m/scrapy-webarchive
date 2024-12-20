from __future__ import annotations

import gzip
from collections import defaultdict
from io import BytesIO

from typing_extensions import IO, Dict, Generator, List, Union
from warc.warc import WARCRecord

from scrapy_webarchive.cdxj import CdxjRecord
from scrapy_webarchive.wacz.constants import ARCHIVE_DIR
from scrapy_webarchive.wacz.storages import ZipStorageHandler
from scrapy_webarchive.warc import WARCReader


class WaczFile:
    """
    Handles looking up pages in the WACZ's index, and iterating over all pages in the index.
    Can also iterate over all entries in each WARC embedded in the archive.
    """

    def __init__(self, storage_handler: ZipStorageHandler):
        self.storage_handler = storage_handler
        self.index = self._parse_index(self._get_index())

    def _find_in_index(self, url: str) -> Union[CdxjRecord, None]:
        """Looks for the most relevant CDXJ record for a given URL in the index."""

        records = self.index.get(url, [])

        # If multiple entries are present, the last one is most likely to be relevant
        return records[-1] if records else None

    def get_warc_from_cdxj_record(self, cdxj_record: CdxjRecord) -> Union[WARCRecord, None]:
        """Retrieves a WARC record from the WACZ archive using a CDXJ record."""

        offset = int(cdxj_record.data["offset"])
        size = int(cdxj_record.data["length"])

        try:    
            file_part = self.storage_handler.fetch_file_part(
                file_name=ARCHIVE_DIR + cdxj_record.data["filename"],
                offset=offset,
                size=size,
            )
        except FileNotFoundError:
            return None

        warc_part = BytesIO(file_part)
        return WARCReader(warc_part).read_record()

    def get_warc_from_url(self, url: str) -> Union[WARCRecord, None]:
        """Retrieves a WARC record from the WACZ archive by searching for the URL in the index."""

        cdxj_record = self._find_in_index(url)
        return self.get_warc_from_cdxj_record(cdxj_record) if cdxj_record else None

    def iter_index(self) -> Generator[CdxjRecord, None, None]:
        """Iterates over all CDXJ records in the WACZ index."""

        for cdxj_records in self.index.values():
            for cdxj_record in cdxj_records:
                yield cdxj_record

    def _get_index(self) -> Union[gzip.GzipFile, IO]:
        """Opens the index file from the WACZ archive, checking for .cdxj, .cdxj.gz, .cdx. and .cdx.gz"""

        index_paths = [
            "indexes/index.cdxj",
            "indexes/index.cdxj.gz",
            "indexes/index.cdx",
            "indexes/index.cdx.gz",
        ]

        # Try opening each possible index file until we've found the index.
        for index_path in index_paths:
            try:
                fetched = self.storage_handler.fetch_file(index_path)

                if index_path.endswith(".gz"):
                    return gzip.open(BytesIO(fetched))
                else:
                    return BytesIO(fetched)

            except FileNotFoundError:
                continue

        raise FileNotFoundError("No valid index file found in WACZ file")

    def _parse_index(self, index_file: Union[gzip.GzipFile, IO]) -> Dict[str, List[CdxjRecord]]:
        cdxj_records = defaultdict(list)

        for line in index_file:
            cdxj_record = CdxjRecord.from_cdxline(line.decode(), wacz_file=self)
            cdxj_records[cdxj_record.data["url"]].append(cdxj_record)

        return cdxj_records


class MultiWaczFile:
    """Supports the same things as WaczFile, but handles multiple WACZ files underneath."""

    def __init__(self, wacz_files: List[WaczFile]) -> None:
        self.waczs = wacz_files

    def get_warc_from_cdxj_record(self, cdxj_record: CdxjRecord) -> Union[WARCRecord, None]:
        """Retrieves a WARC record from the WACZ file corresponding to the given `CdxjRecord`."""

        return cdxj_record.wacz_file.get_warc_from_cdxj_record(cdxj_record) if cdxj_record.wacz_file else None
        
    def get_warc_from_url(self, url: str) -> Union[WARCRecord, None]:
        """Searches through all WACZ files to find a WARC record that matches the provided URL."""

        for wacz in self.waczs:
            warc_record = wacz.get_warc_from_url(url)
            if warc_record:
                return warc_record

        return None

    def iter_index(self) -> Generator[CdxjRecord, None, None]:
        """
        Iterates over the index entries in all WACZ files, yielding `CdxjRecord` objects. Each yielded 
        record has its `wacz_file` attribute set to the corresponding WACZ file.
        """

        yield from (cdxj_record for wacz in self.waczs for cdxj_record in wacz.iter_index())
