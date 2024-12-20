from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from cdxj_indexer.main import CDXJIndexer
from typing_extensions import TYPE_CHECKING, List

if TYPE_CHECKING:
    from scrapy_webarchive.wacz.wacz_file import WaczFile

CDXREC = re.compile(
    r"^(?P<surt>(?P<host>[^\)\s]+)\)(?P<path>[^\?\s]+)?(\?(?P<query>\S+))?)"
    r"\s(?P<datetime>(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})(?:\d{3})?)"
    r"\s(?P<data>{.*})"
)


@dataclass
class CdxjRecord:
    """Represents a CDXJ record, which contains metadata about archived web content stored in WARC files."""

    wacz_file: "WaczFile"
    surt: str
    host: str
    path: str = ""
    query: str = ""
    datetime: str = ""
    year: str = ""
    month: str = ""
    day: str = ""
    hour: str = ""
    minute: str = ""
    second: str = ""
    data: dict = field(default_factory=dict)

    @staticmethod
    def _parse(line: str):
        """Parses a single line from a CDXJ index."""

        return CDXREC.match(line)

    @classmethod
    def from_cdxline(cls, cdxline: str, wacz_file: "WaczFile"):
        """Creates a CdxjRecord instance from a CDX(J) line."""

        m = cls._parse(cdxline.strip())

        if not m:
            raise ValueError(f"Invalid CDXJ line: '{cdxline.strip()}'")

        parsed_data = m.groupdict(default="")
        parsed_data['data'] = json.loads(parsed_data['data'])

        return cls(**parsed_data, wacz_file=wacz_file)

    def __str__(self):
        return str(self.__dict__)


def write_cdxj_index(output: str, inputs: List[str]) -> str:
    """Generates a CDXJ index from a list of input WARC files and writes the index to an output file."""

    wacz_indexer = CDXJIndexer(output=output, inputs=inputs)
    wacz_indexer.process_all()
    return output
