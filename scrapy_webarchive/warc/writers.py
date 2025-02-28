from __future__ import annotations

import uuid
from io import BytesIO
from urllib.parse import urlparse

from scrapy import __version__ as scrapy_version
from scrapy.http.request import Request
from scrapy.http.response import Response
from typing_extensions import List, Tuple, Union
from warcio.recordloader import ArcWarcRecord
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from scrapy_webarchive.warc.utils import generate_warc_fname


class WarcFileWriter:
    """Handles writing WARC files."""

    WARC_VERSION = WARCWriter.WARC_1_1

    def __init__(self, collection_name: str, warc_fname: Union[str, None] = None) -> None:
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
            writer = WARCWriter(fh, gzip=True, warc_version=self.WARC_VERSION)
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
            protocol=self.WARC_VERSION,
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
            protocol=self.WARC_VERSION,
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
            "format": f"WARC file version {self._warc_version_number}",
            "conformsTo": f"https://iipc.github.io/warc-specifications/specifications/warc-format/warc-{self._warc_version_number}/",
            "isPartOf": self.collection_name,
            "robots": "obey" if robotstxt_obey else "ignore",
        }

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            record = writer.create_warcinfo_record(filename=self.warc_fname, info=content)
            writer.write_record(record)

    @property
    def _warc_version_number(self):
        return self.WARC_VERSION.split("/")[1]

    @staticmethod
    def __record_id() -> str:
        """Returns WARC-Record-ID (globally unique UUID) as a string."""

        return f"<urn:uuid:{uuid.uuid1()}>"
