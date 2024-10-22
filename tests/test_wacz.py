import zipfile
from unittest.mock import Mock

import pytest
from freezegun import freeze_time

from scrapy_webarchive.wacz import WaczFileCreator


class TestWaczFileCreator:
    warc_fname = "example-20241007000000-00000-test.warc"
    cdxj_fname = "index.cdxj"
    collection_name = "example"

    @pytest.fixture
    def wacz_file_creator(self):
        """Fixture to initialize the WaczFileCreator with a mocked store"""

        return WaczFileCreator(
            store=Mock(),
            warc_fname=self.warc_fname,
            collection_name=self.collection_name,
            cdxj_fname=self.cdxj_fname,
        )

    @freeze_time("2024-10-04 08:27:11")
    def test_create_wacz(self, fs, wacz_file_creator):
        # Setup the fake filesystem
        fs.create_file(self.cdxj_fname, contents="")
        fs.create_file(self.warc_fname, contents="")

        wacz_file_creator.create()

        # Ensure the files are removed after creation
        assert not fs.exists(self.cdxj_fname)
        assert not fs.exists(self.warc_fname)

        # Verify the WACZ file was persisted in the store
        wacz_fname = wacz_file_creator.get_wacz_fname()
        wacz_file_creator.store.persist_file.assert_called_once()

        # Assert that the correct WACZ filename was used
        assert wacz_fname == f"{self.collection_name}-20241004082711.wacz"

        # Retrieve the zip buffer from the call args
        call_args = wacz_file_creator.store.persist_file.call_args
        zip_buffer = call_args[1]['buf']

        # Verify that the WACZ zip content is correct
        zip_file = zipfile.ZipFile(zip_buffer)
        assert f"indexes/{self.cdxj_fname}" in zip_file.namelist()
        assert f"archive/{self.warc_fname}" in zip_file.namelist()
