import gzip
import socket

import pytest
from freezegun import freeze_time
from scrapy import Request
from scrapy.http import HtmlResponse
from warc.warc import WARCRecord

from scrapy_webarchive.exceptions import WaczMiddlewareException
from scrapy_webarchive.warc import WarcFileWriter, generate_warc_fname, record_transformer


@freeze_time("2024-10-04 08:27:11")
def test_generate_warc_fname(monkeypatch):
    prefix = "rec"
    monkeypatch.setattr(socket, "gethostname", lambda: "example.local")
    assert generate_warc_fname(prefix) == "rec-20241004082711-00000-example.warc.gz"


@pytest.fixture
def warc_record_response():
    payload = b"""HTTP/1.0 200\r\nContent-Length: 11064\r\nDate: Mon, 07 Oct 2024 09:58:44 GMT\r\nContent-Type: text/html; charset=utf-8\r\nStrict-Transport-Security: max-age=0; includeSubDomains; preload\r\n\r\n<!DOCTYPE html>\n<html lang="en">Welcome to scrapy-webarchive!</html>"""
    return WARCRecord(payload=payload, headers={"WARC-Target-URI": "https://quotes.toscrape.com/"})


@pytest.fixture
def warc_record_request():
    return WARCRecord(payload=b"Welcome to scrapy-webarchive!", headers={"WARC-Type": "request"}) 


class TestWarcRecordTransformer:
    def test_request_for_record(self):
        record = {
            "url": "https://quotes.toscrape.com/", 
            "mime": "text/html", 
            "status": "200", 
            "digest": "sha1:AA7J5JETQ4H7GG22MU2NCAUO6LM2EPEU", 
            "length": "2302", 
            "offset": "384", 
            "filename": "quotes-20241007095844-00000-BA92-CKXFG4FF6H.warc.gz",
        }
        
        request = record_transformer.request_for_record(record)
        assert isinstance(request, Request)
        assert request.url == "https://quotes.toscrape.com/"
        assert request.method == "GET"

    def test_response_for_record_invalid_response_type(self, warc_record_request):
        with pytest.raises(WaczMiddlewareException):
            record_transformer.response_for_record(warc_record_request)

    def test_response_for_record(self, warc_record_response):
        response = record_transformer.response_for_record(warc_record_response)
        assert isinstance(response, HtmlResponse)
        assert response.url == 'https://quotes.toscrape.com/'
        assert response.status == 200
        assert response.body == b'<!DOCTYPE html>\n<html lang="en">Welcome to scrapy-webarchive!</html>'


class TestWarcFileWriter:
    warc_fname = '/tmp/test.warc.gz'

    def test_write_warcinfo_record(self, fs):
        fs.create_file(self.warc_fname)
        collection_name = 'example'
        wfw = WarcFileWriter(collection_name=collection_name, warc_fname=self.warc_fname)

        # Confidence test
        warcinfo = gzip.open('/tmp/test.warc.gz', 'rb').read().decode()
        assert warcinfo == ''

        # Write warcinfo record and check output
        wfw.write_warcinfo(robotstxt_obey=True)
        warcinfo = gzip.open('/tmp/test.warc.gz', 'rb').read().decode()
        
        assert 'WARC/1.0' in warcinfo
        assert 'WARC-Type: warcinfo' in warcinfo
        assert 'WARC-Record-ID:' in warcinfo
        assert 'WARC-Filename: /tmp/test.warc.gz' in warcinfo
        assert 'Content-Type: application/warc-fields' in warcinfo
        assert f'isPartOf: {collection_name}' in warcinfo
        assert 'robots: obey' in warcinfo
