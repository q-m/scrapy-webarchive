from __future__ import annotations

import gzip
import io
import json
import os
import zipfile
from collections import defaultdict
from functools import partial
from typing import Any

from scrapy import __version__ as scrapy_version
from scrapy.settings import Settings
from smart_open import open as smart_open
from typing_extensions import IO, TYPE_CHECKING, Dict, Generator, List, Union
from warc.warc import WARCRecord

from scrapy_webarchive import __version__ as scrapy_webarchive_version
from scrapy_webarchive.cdxj import CdxjRecord, write_cdxj_index
from scrapy_webarchive.utils import (
    TIMESTAMP_DT_FORMAT,
    WARC_DT_FORMAT,
    add_ftp_credentials,
    get_formatted_dt_string,
    get_gcs_client,
    get_s3_client,
    get_scheme_from_uri,
    hash_stream,
)
from scrapy_webarchive.warc import WARCReader

if TYPE_CHECKING:
    from scrapy_webarchive.extensions import FilesStoreProtocol


WACZ_VERSION = "1.1.1"

class WaczFileCreator:
    """Handles creating WACZ archives."""

    hash_type = "sha256"
    datapackage_fname = "datapackage.json"

    def __init__(
        self, 
        store: 'FilesStoreProtocol', 
        warc_fname: str, 
        collection_name: str, 
        title: str, 
        description: str, 
        wacz_fname: Union[str, None], 
        cdxj_fname: str = "index.cdxj",
    ) -> None:
        self.store = store
        self.warc_fname = warc_fname
        self.cdxj_fname = cdxj_fname
        self.collection_name = collection_name
        self._title = title
        self._description = description
        self.wacz_fname = wacz_fname or self.get_wacz_fname()

    def create(self) -> None:
        """Create the WACZ file from the WARC and CDXJ index and save it in the configured store."""

        # Write cdxj index to a temporary file
        write_cdxj_index(output=self.cdxj_fname, inputs=[self.warc_fname])

        # Create the WACZ archive in memory
        zip_buffer = self.create_wacz_zip()

        # Cleanup the temporary files
        self.cleanup_files(self.cdxj_fname, self.warc_fname)

        # Save WACZ to the storage
        zip_buffer.seek(0)
        self.store.persist_file(path=self.wacz_fname, buf=zip_buffer, info=None)

    def create_wacz_zip(self) -> io.BytesIO:
        """Create the WACZ zip file and return the in-memory buffer."""

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            self.write_to_zip(zip_file, self.cdxj_fname, "indexes/")
            self.write_to_zip(zip_file, self.warc_fname, "archive/")
            self.write_datapackage(zip_file)

        return zip_buffer

    def write_to_zip(self, zip_file: zipfile.ZipFile, filename: str, destination: str) -> None:
        """Helper function to write a file into the ZIP archive."""

        with open(filename, "rb") as file_handle:
            zip_file.writestr(destination + os.path.basename(filename), file_handle.read())

    def cleanup_files(self, *files: str) -> None:
        """Remove files from the filesystem."""

        for file in files:
            os.remove(file)

    def get_wacz_fname(self) -> str:
        """Generate WACZ filename based on the WARC filename."""

        return f"{self.collection_name}-{get_formatted_dt_string(format=TIMESTAMP_DT_FORMAT)}.wacz"
    
    def write_datapackage(self, zip_file: zipfile.ZipFile) -> None:
        """Main function to create and write the datapackage.json."""

        package_dict = self.create_package_dict()

        with zip_file.open("archive/" + self.warc_fname) as warc_fh:
            package_dict = self.update_package_metadata_from_warc(warc_fh, package_dict)

        package_dict["resources"] = self.collect_resources(zip_file)

        zip_file.writestr(self.datapackage_fname, json.dumps(package_dict, indent=2))

    def create_package_dict(self) -> Dict[str, Any]:
        """Creates the initial package dictionary."""

        dt_string = get_formatted_dt_string(format=WARC_DT_FORMAT)
        return {
            "profile": "data-package",
            "title": self.title,
            "description": self.description,
            "created": dt_string,
            "modified": dt_string,
            "wacz_version": WACZ_VERSION,
            "software": f"scrapy-webarchive/{scrapy_webarchive_version}, Scrapy/{scrapy_version}",
        }

    def update_package_metadata_from_warc(self, warc_fh: IO, package_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Updates the package dictionary with metadata from the WARC records."""

        warc_reader = WARCReader(gzip.open(warc_fh)) if self.warc_fname.endswith(".gz") else WARCReader(warc_fh)

        while True:
            warc_record = warc_reader.read_record()
            if warc_record is None:
                break
            
            if warc_record.type == "request":
                package_dict.update({
                    "mainPageUrl": warc_record.url,
                    "mainPageDate": warc_record.date,
                })
                break

        return package_dict

    def collect_resources(self, zip_file: zipfile.ZipFile) -> List[Dict[str, Any]]:
        """Collects resource information from the zip file."""

        resources = []

        for zip_entry in zip_file.infolist():
            with zip_file.open(zip_entry, "r") as stream:
                size, hash_ = hash_stream(self.hash_type, stream)

                resources.append({
                    "name": os.path.basename(zip_entry.filename).lower(),
                    "path": zip_entry.filename,
                    "hash": hash_,
                    "bytes": size,
                })

        return resources
    
    @property
    def title(self):
        return self._title or self.collection_name

    @property
    def description(self):
        return (
            self._description
            or f"This is the web archive generated by a scrapy-webarchive extension for the {self.collection_name} "
            "spider. It is mainly for scraping purposes as it does not contain any js/css data. Though it can be "
            "replayed as bare HTML if the site does not depend on JavaScript."
        )


class WaczFile:
    """
    Handles looking up pages in the WACZ's index, and iterating over all pages in the index.
    Can also iterate over all entries in each WARC embedded in the archive.
    """

    def __init__(self, file: IO):
        self.wacz_file = zipfile.ZipFile(file)
        self.index = self._parse_index(self._get_index(self.wacz_file))

    def _find_in_index(self, url: str) -> Union[CdxjRecord, None]:
        """Looks for the most relevant CDXJ record for a given URL in the index."""

        records = self.index.get(url, [])

        # If multiple entries are present, the last one is most likely to be relevant
        return records[-1] if records else None

    def get_warc_from_cdxj_record(self, cdxj_record: CdxjRecord) -> Union[WARCRecord, None]:
        """Retrieves a WARC record from the WACZ archive using a CDXJ record."""

        warc_file: Union[gzip.GzipFile, IO]

        try:
            warc_file = self.wacz_file.open("archive/" + cdxj_record.data["filename"])
        except KeyError:
            return None

        warc_file.seek(int(cdxj_record.data["offset"]))
        if cdxj_record.data["filename"].endswith(".gz"):
            warc_file = gzip.open(warc_file)

        return WARCReader(warc_file).read_record()

    def get_warc_from_url(self, url: str) -> Union[WARCRecord, None]:
        """Retrieves a WARC record from the WACZ archive by searching for the URL in the index."""

        cdxj_record = self._find_in_index(url)
        return self.get_warc_from_cdxj_record(cdxj_record) if cdxj_record else None

    def iter_index(self) -> Generator[CdxjRecord, None, None]:
        """Iterates over all CDXJ records in the WACZ index."""

        for cdxj_records in self.index.values():
            for cdxj_record in cdxj_records:
                yield cdxj_record

    @staticmethod
    def _get_index(wacz_file: zipfile.ZipFile) -> Union[gzip.GzipFile, IO]:
        """Opens the index file from the WACZ archive, checking for .cdxj, .cdxj.gz, .cdx. and .cdx.gz"""

        index_paths = [
            "indexes/index.cdxj",
            "indexes/index.cdxj.gz",
            "indexes/index.cdx",
            "indexes/index.cdx.gz",
        ]

        # Try opening each possible index file
        for index_path in index_paths:
            try:
                if index_path.endswith(".gz"):
                    return gzip.open(wacz_file.open(index_path))
                else:
                    return wacz_file.open(index_path)
            except KeyError:
                # Try the next file if this one is not found
                continue

        raise FileNotFoundError(f"No valid index file found in WACZ file: {wacz_file.filename}")

    def _parse_index(self, index_file: Union[gzip.GzipFile, IO]) -> Dict[str, List[CdxjRecord]]:
        cdxj_records = defaultdict(list)

        for line in index_file:
            cdxj_record = CdxjRecord.from_cdxline(line.decode(), wacz_file=self)
            cdxj_records[cdxj_record.data["url"]].append(cdxj_record)

        return cdxj_records


class MultiWaczFile:
    """Supports the same things as WaczFile, but handles multiple WACZ files underneath."""

    def __init__(self, wacz_files: List[IO]) -> None:
        self.waczs = [WaczFile(wacz_file) for wacz_file in wacz_files]

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


def open_wacz_file(wacz_uri: str, timeout: float, settings: Settings) -> Union[IO, None]:
    """Open a WACZ file from a remote location, supporting S3, GCS, and FTP."""
    
    tp = {"timeout": timeout}
    scheme = get_scheme_from_uri(wacz_uri)

    # Map schemes to client creation functions
    scheme_client_map = {
        "s3": partial(get_s3_client, settings),
        "gs": partial(get_gcs_client, settings),
    }

    # Handle clients for specific schemes using the map
    if scheme in scheme_client_map:
        tp["client"] = scheme_client_map[scheme]()
    elif scheme == "ftp":
        wacz_uri = add_ftp_credentials(wacz_uri, settings)

    # Try opening the WACZ file
    try:
        return smart_open(wacz_uri, "rb", transport_params=tp)
    except OSError:
        return None
