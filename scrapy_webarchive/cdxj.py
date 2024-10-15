import json
import re
from dataclasses import dataclass, field
from typing import Any, List

from cdxj_indexer.main import CDXJIndexer

CDXREC = re.compile(
    r"^(?P<surt>(?P<host>[^\)\s]+)\)(?P<path>[^\?\s]+)?(\?(?P<query>\S+))?)"
    r"\s(?P<datetime>(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})(?:\d{3})?)"
    r"\s(?P<data>{.*})"
)


@dataclass
class CdxjRecord:
    wacz_file: Any
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
        return CDXREC.match(line)

    @classmethod
    def from_cdxline(cls, cdxline: str, wacz_file):
        m = cls._parse(cdxline.strip())

        if not m:
            raise ValueError(f"Invalid CDXJ line: '{cdxline.strip()}'")

        parsed_data = m.groupdict(default="")
        parsed_data['data'] = json.loads(parsed_data['data'])

        return cls(**parsed_data, wacz_file=wacz_file)

    def __str__(self):
        return str(self.__dict__)


def write_cdxj_index(output: str, inputs: List[str]) -> str:
    wacz_indexer = CDXJIndexer(output=output, inputs=inputs)
    wacz_indexer.process_all()
    return output
