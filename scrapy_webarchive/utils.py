from datetime import datetime, timezone

WARC_DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIMESTAMP_DT_FORMAT = "%Y%m%d%H%M%S"


def get_current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime(TIMESTAMP_DT_FORMAT)


def get_warc_date() -> str:
    return datetime.now(timezone.utc).strftime(WARC_DT_FORMAT)


def header_lines_to_dict(lines):
    # XXX only supports each header appearing once, not multiple occurences
    headers = {}
    for line in lines:
        k, v = line.split(b":", 1)
        v = v.lstrip()
        headers[k] = v
    return headers
