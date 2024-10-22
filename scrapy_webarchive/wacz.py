import gzip
import io
import os
import zipfile
from collections import defaultdict
from functools import partial
from typing import IO, TYPE_CHECKING, Dict, Generator, List, Union

from scrapy.settings import Settings
from smart_open import open as smart_open
from warc.warc import WARCRecord

from scrapy_webarchive.cdxj import CdxjRecord, write_cdxj_index
from scrapy_webarchive.utils import (
    add_ftp_credentials,
    get_current_timestamp,
    get_gcs_client,
    get_s3_client,
    get_scheme_from_uri,
)
from scrapy_webarchive.warc import WARCReader

if TYPE_CHECKING:
    from scrapy_webarchive.extensions import FilesStoreProtocol


class WaczFileCreator:
    """Handles creating WACZ archives."""

    def __init__(self, store: 'FilesStoreProtocol', warc_fname: str, collection_name: str, cdxj_fname: str = "index.cdxj") -> None:
        self.store = store
        self.warc_fname = warc_fname
        self.cdxj_fname = cdxj_fname
        self.collection_name = collection_name

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
        self.store.persist_file(path=self.get_wacz_fname(), buf=zip_buffer, info=None)

    def create_wacz_zip(self) -> io.BytesIO:
        """Create the WACZ zip file and return the in-memory buffer."""

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            self.write_to_zip(zip_file, self.cdxj_fname, "indexes/")
            self.write_to_zip(zip_file, self.warc_fname, "archive/")

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

        return f"{self.collection_name}-{get_current_timestamp()}.wacz"


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
