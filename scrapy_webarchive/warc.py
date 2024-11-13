from __future__ import annotations

import socket
import uuid
from io import BytesIO
from urllib.parse import urlparse

from scrapy import __version__ as scrapy_version
from scrapy.http.request import Request
from scrapy.http.response import Response
from scrapy.responsetypes import ResponseTypes
from typing_extensions import List, Optional, Tuple
from warc import WARCReader as BaseWARCReader
from warc.warc import WARCRecord
from warcio.recordloader import ArcWarcRecord
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from scrapy_webarchive.cdxj import CdxjRecord
from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.utils import TIMESTAMP_DT_FORMAT, get_formatted_dt_string, header_lines_to_dict


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


class WARCReader(BaseWARCReader):
    """WARC reader with compatibility for WARC version 1.0 and 1.1."""

    SUPPORTED_VERSIONS = ["1.0", "1.1"]


class WarcFileWriter:
    """Handles writing WARC files."""

    def __init__(self, collection_name: str, warc_fname: Optional[str] = None) -> None:
        self.collection_name = collection_name
        self.warc_fname = warc_fname or generate_warc_fname(prefix=collection_name)

    def write_record(
        self, 
        url: str, 
        record_type: str, 
        headers: List[Tuple[str, str]], 
        warc_headers: StatusAndHeaders, 
        content_type: str, 
        content: bytes, 
        http_line: str,
    ) -> ArcWarcRecord:
        """Write any WARC record (response or request) to a WARC file."""

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            http_headers = StatusAndHeaders(statusline=http_line, headers=headers, is_http_request=True)
            payload = BytesIO(content)

            record = writer.create_warc_record(
                uri=url,
                record_type=record_type,
                payload=payload,
                http_headers=http_headers,
                warc_headers=warc_headers,
                warc_content_type=content_type,
            )
            writer.write_record(record)
        
        return record

    def write_response(self, response: Response, request: Request) -> ArcWarcRecord:
        """Write a WARC-Type: response record."""

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

        record = self.write_record(
            url=response.url,
            record_type="response",
            content=response.body,
            content_type="application/http; msgtype=response",
            headers=headers,
            warc_headers=warc_headers,
            http_line=http_line,
        )
        return record

    def write_request(self, request: Request, concurrent_to: ArcWarcRecord):
        """Write a WARC-Type: request record."""

        record_id = self.__record_id()
        warc_headers = StatusAndHeaders(
            "",
            [
                ("WARC-Type", "request"),
                ("WARC-Target-URI", request.url),
                ("WARC-Date", request.meta["WARC-Date"]),
                ("WARC-Record-ID", record_id),
                ("WARC-Concurrent-To", concurrent_to.rec_headers.get_header('WARC-Record-ID')),
            ],
            protocol="WARC/1.0",
        )

        http_line = f"{request.method} {urlparse(request.url).path} HTTP/1.0"

        headers = []
        for key in request.headers:
            val = request.headers[key]
            headers.append((key.decode(), val.decode()))

        record = self.write_record(
            url=request.url,
            record_type="request",
            content_type="application/http; msgtype=request",
            content=request.body,
            headers=headers,
            warc_headers=warc_headers,
            http_line=http_line,
        )
        return record

    def write_warcinfo(self, robotstxt_obey: bool) -> None:
        """Write WARC-Type: warcinfo record."""

        content = {
            "software": f"Scrapy/{scrapy_version} (+https://scrapy.org)",
            "format": "WARC file version 1.0",
            "conformsTo": "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/",
            "isPartOf": self.collection_name,
            "robots": "obey" if robotstxt_obey else "ignore",
        }

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            record = writer.create_warcinfo_record(filename=self.warc_fname, info=content)
            writer.write_record(record)

    @staticmethod
    def __record_id() -> str:
        """Returns WARC-Record-ID (globally unique UUID) as a string."""

        return f"<urn:uuid:{uuid.uuid1()}>"


class WarcRecordTransformer:
    """A helper class to convert WARC records into Scrapy requests and responses."""
    
    response_types = ResponseTypes()

    def request_for_record(self, cdxj_record: CdxjRecord, **kwargs):
        """Create a Scrapy request instance from a WARCRecord."""

        # TODO: locate request in WACZ and include all relevant things (like headers)
        return Request(url=cdxj_record.data["url"], method=cdxj_record.data.get("method", "GET"), **kwargs)

    def response_for_record(self, warc_record: WARCRecord, **kwargs):
        """Create a Scrapy response instance from a WARCRecord."""

        # We expect a response.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-type-mandatory
        if warc_record.type != "response":
            raise WaczMiddlewareException(f"Unexpected record type: {warc_record.type}")

        # We only know how to handle application/http.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        record_content_type = (warc_record["Content-Type"] or "").split(";", 1)[0]
        if record_content_type != "application/http":
            raise WaczMiddlewareException(f"Unexpected record content-type: {record_content_type}")

        # There is a date field in record['WARC-Date'], but don't have a use for it now.
        # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-date-mandatory

        payload = warc_record.payload.read()
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
            url=warc_record.url,
            status=int(status.decode()),
            protocol=protocol.decode(),
            headers=headers,
            body=body,
            **kwargs,
        )


record_transformer = WarcRecordTransformer()
