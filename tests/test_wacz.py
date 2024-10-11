import zipfile
from unittest.mock import Mock

import pytest

from scrapy_webarchive.wacz import WaczFileCreator


class TestWaczFileCreator:
    @pytest.fixture
    def wacz_file_creator(self):
        """Fixture to initialize the WaczFileCreator with a mocked store"""

        store = Mock()
        warc_fname = "/scrapy-webarchive/quotes-20241007000000-00000-test.warc"
        cdxj_fname = "/scrapy-webarchive/index.cdxj"
        return WaczFileCreator(store=store, warc_fname=warc_fname, cdxj_fname=cdxj_fname)

    def test_create_wacz(self, fs, wacz_file_creator):
        # Setup the fake filesystem
        fs.create_file("/scrapy-webarchive/index.cdxj", contents="")
        fs.create_file("/scrapy-webarchive/quotes-20241007000000-00000-test.warc", contents="")

        wacz_file_creator.create()

        # Ensure the files are removed after creation
        assert not fs.exists("/scrapy-webarchive/index.cdxj")
        assert not fs.exists("/scrapy-webarchive/quotes-20241007000000-00000-test.warc")

        # Verify the WACZ file was persisted in the store
        wacz_fname = wacz_file_creator.get_wacz_fname()
        wacz_file_creator.store.persist_file.assert_called_once()

        # Assert that the correct WACZ filename was used
        assert wacz_fname == "/scrapy-webarchive/quotes-20241007000000.wacz"

        # Retrieve the zip buffer from the call args
        call_args = wacz_file_creator.store.persist_file.call_args
        zip_buffer = call_args[0][1]

        # Verify that the WACZ zip content is correct
        zip_file = zipfile.ZipFile(zip_buffer)
        assert "indexes/index.cdxj" in zip_file.namelist()
        assert "archive/quotes-20241007000000-00000-test.warc" in zip_file.namelist()
