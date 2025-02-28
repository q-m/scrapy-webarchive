from scrapy.http.request import Request
from scrapy.responsetypes import ResponseTypes
from warc.warc import WARCRecord

from scrapy_webarchive.cdxj.models import CdxjRecord
from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.utils import header_lines_to_dict


class WarcRecordTransformer:
    """A helper class to convert WARC records into Scrapy requests and responses."""
    
    response_types = ResponseTypes()

    def request_for_record(self, cdxj_record: CdxjRecord, **kwargs):
        """Create a Scrapy request instance from a WARCRecord."""

        # TODO: locate request in WACZ and include all relevant things (like headers)
        return Request(url=cdxj_record.data["url"], method=cdxj_record.data.get("method", "GET"), **kwargs)

    def response_for_record(self, warc_record: WARCRecord, request: Request, **kwargs):
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
            request=request,
            status=int(status.decode()),
            protocol=protocol.decode(),
            headers=headers,
            body=body,
            **kwargs,
        )


record_transformer = WarcRecordTransformer()
