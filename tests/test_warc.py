import gzip
import socket
from io import BytesIO

import pytest
from freezegun import freeze_time
from scrapy.http import HtmlResponse
from scrapy.http.request import Request
from scrapy.http.response import Response
from warc.warc import WARCRecord
from warcio.recordloader import ArcWarcRecordLoader

from scrapy_webarchive.cdxj import CdxjRecord
from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.warc import WarcFileWriter, generate_warc_fname, record_transformer


@freeze_time("2024-10-04 08:27:11")
def test_generate_warc_fname(monkeypatch):
    prefix = "example"
    monkeypatch.setattr(socket, "gethostname", lambda: "example.local")
    assert generate_warc_fname(prefix) == "example-20241004082711-00000-example.warc.gz"


@pytest.fixture
def warc_record_response():
    payload = (
        b"HTTP/1.0 200\r\nContent-Length: 11064\r\nDate: Mon, 07 Oct 2024 09:58:44 GMT\r\nContent-Type: text/html; "
        b"charset=utf-8\r\nStrict-Transport-Security: max-age=0; includeSubDomains; preload\r\n\r\n<!DOCTYPE html>\n"
        b"<html lang=\"en\">Welcome to scrapy-webarchive!</html>"
    )
    return WARCRecord(payload=payload, headers={"WARC-Target-URI": "http://example.com"})


@pytest.fixture
def warc_record_request():
    return WARCRecord(payload=b"Welcome to scrapy-webarchive!", headers={"WARC-Type": "request"}) 


class TestWarcRecordTransformer:
    def test_request_for_record(self):
        record = CdxjRecord(
            wacz_file=None,
            surt="com,example)/index",
            host="example",
            data={
            "url": "http://example.com", 
                "mime": "text/html", 
                "status": "200", 
                "digest": "sha1:AA7J5JETQ4H7GG22MU2NCAUO6LM2EPEU", 
                "length": "2302", 
                "offset": "384", 
                "filename": "example-20241007095844-00000-BA92-CKXFG4FF6H.warc.gz",
            }
        )
        
        request = record_transformer.request_for_record(record)
        assert isinstance(request, Request)
        assert request.url == "http://example.com"
        assert request.method == "GET"

    def test_response_for_record_invalid_response_type(self, warc_record_request):
        with pytest.raises(WaczMiddlewareException):
            record_transformer.response_for_record(warc_record_request)

    def test_response_for_record(self, warc_record_response):
        response = record_transformer.response_for_record(warc_record_response)
        assert isinstance(response, HtmlResponse)
        assert response.url == "http://example.com"
        assert response.status == 200
        assert response.body == b'<!DOCTYPE html>\n<html lang="en">Welcome to scrapy-webarchive!</html>'


UTF8_PAYLOAD = u'\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Content-Disposition: attachment; filename="example.txt"\r\n\
Custom-Header: somevalue\r\n\
Unicode-Header: %F0%9F%93%81%20text%20%F0%9F%97%84%EF%B8%8F\r\n\
\r\n\
some\n\
text'

content_length = len(UTF8_PAYLOAD.encode('utf-8'))

UTF8_RECORD = u'\
WARC/1.0\r\n\
WARC-Type: response\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:KMUABC6URWIQ7QXCZDQ5FS6WIBBFRORR\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: {0}\r\n\
\r\n\
{1}\r\n\
\r\n\
'.format(content_length, UTF8_PAYLOAD)


class TestWarcFileWriter:
    warc_fname = "/tmp/test.warc.gz"
    collection_name = "example"

    def setup_method(self):
        self.writer = WarcFileWriter(collection_name=self.collection_name, warc_fname=self.warc_fname)

    def test_write_warcinfo_record(self, fs):
        fs.create_file(self.warc_fname)

        # Confidence test
        warcinfo = gzip.open(self.warc_fname, "rb").read().decode()
        assert warcinfo == ""

        # Write warcinfo record and check output
        self.writer.write_warcinfo(robotstxt_obey=True)
        warcinfo = gzip.open(self.warc_fname, "rb").read().decode()
        
        assert "WARC/1.0" in warcinfo
        assert "WARC-Type: warcinfo" in warcinfo
        assert "WARC-Record-ID:" in warcinfo
        assert "WARC-Filename: /tmp/test.warc.gz" in warcinfo
        assert "Content-Type: application/warc-fields" in warcinfo
        assert f"isPartOf: {self.collection_name}" in warcinfo
        assert "robots: obey" in warcinfo

    def test_write_warcrequest_record(self, fs):
        fs.create_file(self.warc_fname)

        # Confidence test
        warc_data = gzip.open(self.warc_fname, 'rb').read().decode()
        assert warc_data == ''

        # Write request record and check output
        request = Request("http://www.example.com", meta={'WARC-Date': '2000-01-01T00:00:00Z'})
        warc_response_record = ArcWarcRecordLoader().parse_record_stream(BytesIO(UTF8_RECORD.encode('utf-8')))
        self.writer.write_request(request=request, concurrent_to=warc_response_record)
        warc_request_record = gzip.open(self.warc_fname, 'rb').read().decode()

        assert 'WARC/1.0' in warc_request_record
        assert 'WARC-Type: request' in warc_request_record
        assert 'WARC-Record-ID:' in warc_request_record
        assert 'WARC-Concurrent-To: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>' in warc_request_record
        assert 'Content-Type: application/http; msgtype=request' in warc_request_record

    def test_write_warcresponse_record(self, fs):
        fs.create_file(self.warc_fname)

        # Confidence test
        warc_data = gzip.open(self.warc_fname, 'rb').read().decode()
        assert warc_data == ''

        # Write response record and check output
        request = Request("http://www.example.com", meta={'WARC-Date': '2000-01-01T00:00:00Z'})
        response = Response("http://www.example.com")
        self.writer.write_response(request=request, response=response)
        warc_response_record = gzip.open(self.warc_fname, 'rb').read().decode()

        assert 'WARC/1.0' in warc_response_record
        assert 'WARC-Type: response' in warc_response_record
        assert 'WARC-Record-ID:' in warc_response_record
        assert 'Content-Type: application/http; msgtype=response' in warc_response_record
