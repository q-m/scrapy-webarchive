import gzip
import io
import os
import zipfile
from collections import defaultdict

from warc import WARCReader as BaseWARCReader

from scrapy_webarchive.cdxj import CdxjRecord, write_cdxj_index
from scrapy_webarchive.utils import get_current_timestamp


class WARCReader(BaseWARCReader):
    """WARC reader with compatibility for WARC version 1.0 and 1.1"""

    SUPPORTED_VERSIONS = ["1.0", "1.1"]


class WaczFileCreator:
    """Handles creating WACZ archives"""

    def __init__(
            self,
            store,
            warc_fname: str, 
            cdxj_fname: str = "index.cdxj", 
        ) -> None:
        self.store = store
        self.warc_fname = warc_fname
        self.cdxj_fname = cdxj_fname

    def create(self):
        """Create the WACZ file from the WARC and CDXJ index"""

        # Write cdxj index to a temporary file
        write_cdxj_index(output=self.cdxj_fname, inputs=[self.warc_fname])

        # Create the WACZ archive in memory
        zip_buffer = self.create_wacz_zip()

        # Cleanup the temporary files
        self.cleanup_files(self.cdxj_fname, self.warc_fname)

        # Save WACZ to the storage
        zip_buffer.seek(0)
        self.store.persist_file(self.get_wacz_fname(), zip_buffer, info=None)

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

        return f"archive-{get_current_timestamp()}.wacz"


class MultiWaczFile:
    """
    The MultiWACZ file format is not yet finalized, hence instead of pointing to a
    MultiWACZ file, this just works with the multiple WACZ files.

    Supports the same things as WACZFile, but handles multiple WACZ files underneath.
    """

    def __init__(self, wacz_files):
        self.waczs = [WaczFile(f) for f in wacz_files]

    def load_index(self):
        for f in self.waczs:
            f.load_index()

    def get_record(self, url_or_record, **kwargs):
        if not isinstance(url_or_record, str):
            return url_or_record["_wacz_file"].get_record(url_or_record, **kwargs)
        for f in self.waczs:
            r = f.get_record(url_or_record, **kwargs)
            if r:
                return r

    def iter_index(self):
        for f in self.waczs:
            for r in f.iter_index():
                yield {**r, "_wacz_file": f}


class WaczFile:
    """
    Handles looking up pages in the index, and iterating over all pages in the index.
    Can also iterate over all entries in each WARC embedded in the archive.
    """

    index = None

    def __init__(self, file):
        self.wacz_file = zipfile.ZipFile(file)

    def _find_in_index(self, url, **kwargs):
        if not self.index:
            self.load_index()

        records = self.index.get(url, [])
        # allow filtering on all given fields
        for k, v in kwargs.items():
            records = [r for r in records if r.get(k) == v]
        if len(records) > 0:
            # if multiple entries are present, the last one is most likely to be relevant
            return records[-1]
        # nothing found
        return None

    def load_index(self):
        self.index = self._parse_index(self._get_index(self.wacz_file))

    def get_record(self, url_or_cdxjrecord, **kwargs):
        if isinstance(url_or_cdxjrecord, str):
            record = self._find_in_index(url_or_cdxjrecord, **kwargs)
            if not record:
                return None
        else:
            record = url_or_cdxjrecord

        try:
            warc_file = self.wacz_file.open("archive/" + record["filename"])
        except KeyError:
            return None
        warc_file.seek(int(record["offset"]))
        if record["filename"].endswith(".gz"):
            warc_file = gzip.open(warc_file)

        reader = WARCReader(warc_file)
        return reader.read_record()

    def iter_index(self):
        if not self.index:
            self.load_index()

        for records in self.index.values():
            for record in records:
                yield record


    @staticmethod
    def _get_index(wacz_file):
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

        raise FileNotFoundError("No valid index file found.")

    @staticmethod
    def _parse_index(index_file):
        records = defaultdict(list)

        for line in index_file:
            record = CdxjRecord(line.decode())
            url = record.data["url"]
            records[url].append(record.data)

        return records
