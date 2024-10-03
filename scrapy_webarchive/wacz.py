import gzip
import io
import os
import zipfile
from collections import defaultdict

from cdxj_indexer.main import CDXJIndexer
from warc import WARCReader

from scrapy_webarchive.cdxj import CdxjRecord


class WaczFileCreator:
    """Handles creating WACZ archives"""

    def __init__(
            self,
            store,
            warc_fname: str, 
            cdxj_fname: str = "index.cdxj", 
            wacz_fname: str = "archive.wacz",
        ):
        self.store = store
        self.warc_fname = warc_fname
        self.cdxj_fname = cdxj_fname
        self.wacz_fname = wacz_fname

    def create_wacz(self) -> None:
        """Create the WACZ file from the WARC"""

        zip_buffer = io.BytesIO()

        # Write index
        wacz_indexer = CDXJIndexer(
            output=self.cdxj_fname,
            inputs=[self.warc_fname],
        )
        wacz_indexer.process_all()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add the cdxj file to the WACZ
            with open(self.cdxj_fname, "rb") as in_fh:
                file_content = in_fh.read()  # Read file content
                zip_file.writestr("indexes/" + os.path.basename(self.cdxj_fname), file_content)

            os.remove(self.cdxj_fname)  # Remove original cdxj file

            # Write WARC file to the WACZ
            with open(self.warc_fname, "rb") as in_fh:
                file_content = in_fh.read()  # Read file content
                zip_file.writestr("archive/" + os.path.basename(self.warc_fname), file_content)

            os.remove(self.warc_fname)  # Remove original WARC file

        zip_buffer.seek(0)
        self.store.persist_file(self.wacz_fname, zip_buffer, info=None)


class MultiWaczFile:
    """
    Multiple WACZ files

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

    def iter_warc(self):
        for f in self.waczs:
            for r in f.iter_warc():
                yield r


class WaczFile:
    """
    WACZ file.

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

    def iter_warc(self):
        for entry in self.wacz_file.infolist():
            if entry.is_dir():
                continue

            if not entry.filename.startswith("archive/"):
                continue

            warc_file = self.wacz_file.open(entry)
            if entry.filename.endswith(".gz"):
                warc_file = gzip.open(warc_file)

            reader = WARCReader(warc_file)
            for record in reader:
                yield record

    @staticmethod
    def _get_index(wacz_file):
        try:
            return wacz_file.open("indexes/index.cdxj")
        except KeyError:
            return gzip.open(wacz_file.open("indexes/index.cdxj.gz"))

    @staticmethod
    def _parse_index(index_file):
        records = defaultdict(list)

        for line in index_file:
            record = CdxjRecord(line.decode())
            url = record.data["url"]
            records[url].append(record.data)

        return records
