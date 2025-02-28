from __future__ import annotations

import socket

from scrapy_webarchive.constants import TIMESTAMP_DT_FORMAT
from scrapy_webarchive.utils import get_formatted_dt_string


def generate_warc_fname(prefix: str) -> str:
    """
    Returns new WARC filename based on recommendation in the warc-specification:
    https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#annex-c-informative-warc-file-size-and-name-recommendations
    {prefix}-{timestamp}-{serial}-{crawlhost}.warc.gz
    """

    timestamp = get_formatted_dt_string(format=TIMESTAMP_DT_FORMAT)
    crawlhost = socket.gethostname().split(".")[0]
    # As of now we only generate one WARC file. Add serial in here to adhere to the warc specification.
    serial = '00000'

    return "-".join([prefix, timestamp, serial, crawlhost]) + ".warc.gz"
