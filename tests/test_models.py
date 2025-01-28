import pytest
from scrapy.http import Request, Response

from scrapy_webarchive.models import WarcMetadata
from scrapy_webarchive.utils import WEBARCHIVE_META_KEY


class TestWarcMetadata:
    def setup_method(self):
        self.meta = {
            "action": "read",
            "record_id": "12345",
            "wacz_uri": "s3://example/archive.wacz",
        }

    def test_warc_metadata_to_dict(self):
        result = WarcMetadata(**self.meta).to_dict()
        assert result == self.meta

    def test_warc_metadata_from_response_valid(self):
        warc_metadata = WarcMetadata.from_response(self._get_response())
        assert isinstance(warc_metadata, WarcMetadata)
        assert warc_metadata.action == "read"
        assert warc_metadata.record_id == "12345"
        assert warc_metadata.wacz_uri == "s3://example/archive.wacz"

    def test_warc_metadata_from_response_no_meta(self):
        response = self._get_response(meta={})
        warc_meta = WarcMetadata.from_response(response)
        assert warc_meta is None

    def test_warc_metadata_from_response_no_warc_meta(self):
        response = self._get_response(meta={"key": "value"})
        warc_meta = WarcMetadata.from_response(response)
        assert warc_meta is None

    def test_warc_metadata_from_response_invalid_meta(self):
        response = self._get_response(meta={WEBARCHIVE_META_KEY: {"invalid_key": "value"}})
        with pytest.raises(TypeError):
            WarcMetadata.from_response(response)
    
    def _get_response(self, meta: dict = None, url: str = "https://example.com"):
        if meta is None:
            meta = {WEBARCHIVE_META_KEY: self.meta}

        request = Request(url=url, meta=meta)
        return Response(url=url, request=request)
