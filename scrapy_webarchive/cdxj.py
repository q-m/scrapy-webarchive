# based on https://github.com/internetarchive/cdx-summary/blob/main/cdxsummary/parser.py
import json
import re

CDXREC = re.compile(
    r"^(?P<surt>(?P<host>[^\)\s]+)\)(?P<path>[^\?\s]+)?(\?(?P<query>\S+))?)"
    r"\s(?P<datetime>(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))"
    r"\s(?P<data>{.*})"
)


class CdxjRecord:
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
