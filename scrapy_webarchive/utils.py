import gzip
import json
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

from warc import WARCReader


def warc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MultiWACZFile:
    """
    Multiple WACZ files

    The MultiWACZ file format is not yet finalized, hence instead of pointing to a
    MultiWACZ file, this just works with the multiple WACZ files.

    Supports the same things as WACZFile, but handles multiple WACZ files underneath.
    """

    waczs = []

    def __init__(self, wacz_files):
        self.waczs = [WACZFile(f) for f in wacz_files]

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


class WACZFile:
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
            record = CDXJRecord(line.decode())
            url = record.data["url"]
            records[url].append(record.data)

        return records


# based on https://github.com/internetarchive/cdx-summary/blob/main/cdxsummary/parser.py

CDXREC = re.compile(
    r"^(?P<surt>(?P<host>[^\)\s]+)\)(?P<path>[^\?\s]+)?(\?(?P<query>\S+))?)"
    r"\s(?P<datetime>(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))"
    r"\s(?P<data>{.*})"
)


class CDXJRecord:
    def _segment_length(self, seg, sep):
        return seg.strip(sep).count(sep) + 1 if seg.strip(sep) else 0

    def _parse(self, line):
        return CDXREC.match(line)

    def __init__(self, cdxline):
        m = self._parse(cdxline.strip())

        if not m:
            raise ValueError(f"Invalid CDXJ line: '{cdxline.strip()}'")

        for key, value in m.groupdict(default="").items():
            if key == "data":
                value = json.loads(value)

            setattr(self, key, value)

    def __str__(self):
        return str(self.__dict__)
