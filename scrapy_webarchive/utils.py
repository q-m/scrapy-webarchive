from datetime import datetime, timezone


def warc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def header_lines_to_dict(lines):
    # XXX only supports each header appearing once, not multiple occurences
    headers = {}
    for line in lines:
        k, v = line.split(b":", 1)
        v = v.lstrip()
        headers[k] = v
    return headers
