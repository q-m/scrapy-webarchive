import os
import socket
import uuid
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urlparse

from scrapy import __version__ as scrapy_version
from scrapy.http.request import Request
from scrapy.responsetypes import ResponseTypes
from warc.warc import WARCRecord
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.utils import header_lines_to_dict


def create_warc_fname(tla):
    """
    Returns new WARC filename. WARC filename format compatible with internetarchive/draintasker warc naming #1:
    {TLA}-{timestamp}-{serial}-{fqdn}.warc.gz

    Raises IOError if WARC file exists or destination is not found.
    """

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    fqdn = socket.gethostname().split(".")[0]
    warc_name = "-".join([tla, timestamp, "00000", fqdn]) + ".warc.gz"
    warc_dest = ""  # TODO

    if warc_dest and not os.path.exists(warc_dest):
        raise IOError(f"warc_dest not found: {warc_dest}")

    fname = os.path.join(warc_dest, warc_name)

    if os.path.exists(fname):
        raise IOError(f"WARC file exists: {fname}")

    return fname


class WarcFileWriter:
    """Handles writing WARC files"""

    def __init__(self, warc_fname: str, collection_name: str):
        self.warc_fname = warc_fname
        self.collection_name = collection_name

    def write_record(
        self, url, record_type, headers, warc_headers, content_type, content, http_line
    ):
        """Write any WARC record (response or request) to a WARC file"""

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            http_headers = StatusAndHeaders(statusline=http_line, headers=headers)
            payload = BytesIO(bytes(content, "utf-8"))

            record = writer.create_warc_record(
                uri=url,
                record_type=record_type,
                payload=payload,
                http_headers=http_headers,
                warc_headers=warc_headers,
                warc_content_type=content_type,
            )
            writer.write_record(record)

    def write_response(self, response, request):
        record_id = self.__record_id()
        warc_headers = StatusAndHeaders(
            "",
            [
                ("WARC-Type", "response"),
                ("WARC-Target-URI", response.url),
                ("WARC-Date", request.meta["WARC-Date"]),
                ("WARC-Record-ID", record_id),
            ],
            protocol="WARC/1.0",
        )

        http_line = f"HTTP/1.0 {str(response.status)}"

        headers = []
        for key in response.headers:
            val = response.headers[key]
            headers.append((key.decode(), val.decode()))

        self.write_record(
            url=response.url,
            record_type="response",
            content=response.body.decode(),
            content_type="application/http; msgtype=response",
            headers=headers,
            warc_headers=warc_headers,
            http_line=http_line,
        )
        return record_id

    def write_request(self, request, concurrent_to):
        """Write a WARC-Type: request record"""

        record_id = self.__record_id()
        warc_headers = StatusAndHeaders(
            "",
            [
                ("WARC-Type", "request"),
                ("WARC-Target-URI", request.url),
                ("WARC-Date", request.meta["WARC-Date"]),
                ("WARC-Record-ID", record_id),
                ("WARC-Concurrent-To", concurrent_to),
            ],
            protocol="WARC/1.0",
        )

        http_line = f"{request.method} {urlparse(request.url).path} HTTP/1.0"

        headers = []
        for key in request.headers:
            val = request.headers[key]
            headers.append((key.decode(), val.decode()))

        self.write_record(
            url=request.url,
            record_type="request",
            content_type="application/http; msgtype=request",
            content=request.body.decode(),
            headers=headers,
            warc_headers=warc_headers,
            http_line=http_line,
        )
        return record_id

    def write_warcinfo(self):
        """Write WARC-Type: warcinfo record"""

        content = {
            "software": f"Scrapy/{scrapy_version} (+https://scrapy.org)",
            "format": "WARC file version 1.0",
            "conformsTo": "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/",
            "isPartOf": self.collection_name,
            "robots": "obey" if True else "ignore",
        }

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            record = writer.create_warcinfo_record(filename=self.warc_fname, info=content)
            writer.write_record(record)

    @staticmethod
    def __record_id():
        """Returns WARC-Record-ID (globally unique UUID) as a string"""
        return f"<urn:uuid:{uuid.uuid1()}>"


class WarcRecordTransformer:
    """
    A helper class to convert WARC records into Scrapy requests and responses.
    """
    
    response_types = ResponseTypes()

    def request_for_record(self, record: WARCRecord, **kwargs):
        # TODO: locate request in WACZ and include all relevant things (like headers)
        return Request(url=record["url"], method=record.get("method", "GET"), **kwargs)

    def response_for_record(self, record: WARCRecord, **kwargs):
        # We expect a response.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-type-mandatory
        if record["WARC-Type"] != "response":
            raise WaczMiddlewareException(f"Unexpected record type: {record['type']}")

        # We only know how to handle application/http.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        record_content_type = (record["Content-Type"] or "").split(";", 1)[0]
        if record_content_type != "application/http":
            raise WaczMiddlewareException(
                f"Unexpected record content-type: {record_content_type}"
            )

        # There is a date field in record['WARC-Date'], but don't have a use for it now.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-date-mandatory

        payload = record.payload.read()
        payload_parts = payload.split(b"\r\n\r\n", 1)
        header_lines = payload_parts[0] if len(payload_parts) > 0 else ""
        body = payload_parts[1] if len(payload_parts) > 1 else None

        header_lines = header_lines.split(b"\r\n")
        header_parts = header_lines[0].split(None, 2)
        protocol = header_parts[0] if len(header_parts) > 0 else None
        status = header_parts[1] if len(header_parts) > 1 else None
        headers = header_lines_to_dict(header_lines[1:])

        if not status or not protocol:
            return None

        response_cls = self.response_types.from_headers(headers)

        return response_cls(
            url=record["WARC-Target-URI"],
            status=int(status.decode()),
            protocol=protocol.decode(),
            headers=headers,
            body=body,
            **kwargs,
        )


record_transformer = WarcRecordTransformer()
