import zipfile
from io import BytesIO
from typing import cast
from unittest.mock import Mock

import pytest
from freezegun import freeze_time
from scrapy import __version__ as scrapy_version

from scrapy_webarchive import __version__ as scrapy_webarchive_version
from scrapy_webarchive.extensions import FilesStoreProtocol
from scrapy_webarchive.wacz.creator import WACZ_VERSION, WaczFileCreator


class TestWaczFileCreator:
    warc_fname = "example-20241007000000-00000-test.warc"
    cdxj_fname = "index.cdxj"
    collection_name = "example"

    @pytest.fixture
    def wacz_file_creator(self):
        """Fixture to initialize the WaczFileCreator with a mocked store"""

        mock_store = cast(FilesStoreProtocol, Mock(spec=FilesStoreProtocol))
        
        return WaczFileCreator(
            store=mock_store,
            warc_fname=self.warc_fname,
            collection_name=self.collection_name,
            cdxj_fname=self.cdxj_fname,
            title="Testing",
            description="WACZ generated durning a unit-test",
            wacz_fname=None,
        )

    @freeze_time("2024-10-04 08:27:11")
    def test_create_wacz(self, fs, wacz_file_creator: WaczFileCreator):
        # Setup the fake filesystem
        fs.create_file(self.cdxj_fname, contents="")
        fs.create_file(self.warc_fname, contents="")

        wacz_file_creator.create()

        # Ensure the files are removed after creation
        assert not fs.exists(self.cdxj_fname)
        assert not fs.exists(self.warc_fname)

        # Verify the WACZ file was persisted in the store
        wacz_fname = wacz_file_creator.get_wacz_fname()
        mock_store = cast(Mock, wacz_file_creator.store)
        mock_store.persist_file.assert_called_once()

        # Assert that the correct WACZ filename was used
        assert wacz_fname == f"{self.collection_name}-20241004082711.wacz"

        # Retrieve the zip buffer from the call args
        call_args = mock_store.persist_file.call_args
        zip_buffer = call_args[1]["buf"]

        # Verify that the WACZ zip content is correct
        zip_file = zipfile.ZipFile(zip_buffer)
        assert f"indexes/{self.cdxj_fname}" in zip_file.namelist()
        assert f"archive/{self.warc_fname}" in zip_file.namelist()
        assert "datapackage.json" in zip_file.namelist()

    @freeze_time("2024-10-04 08:27:11")
    def test_create_package_dict(self, wacz_file_creator: WaczFileCreator):
        package_dict = wacz_file_creator.create_package_dict()

        expected = {
            "profile": "data-package",
            "title": "Testing",
            "description": "WACZ generated durning a unit-test",
            "created": "2024-10-04T08:27:11Z",
            "modified": "2024-10-04T08:27:11Z",
            "wacz_version": WACZ_VERSION,
            "software": f"scrapy-webarchive/{scrapy_webarchive_version}, Scrapy/{scrapy_version}",
        }

        assert package_dict == expected

    def test_package_metadata_from_warc(self, wacz_file_creator: WaczFileCreator, warc_example):
        res = wacz_file_creator.update_package_metadata_from_warc(BytesIO(warc_example), {})

        assert res["mainPageUrl"] == "http://example.com/"
        assert res["mainPageDate"] == "2024-02-10T16:15:52Z"
