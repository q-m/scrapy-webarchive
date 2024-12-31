from __future__ import annotations

import gzip
import io
import json
import os
import zipfile
from typing import Any

from scrapy import __version__ as scrapy_version
from typing_extensions import IO, TYPE_CHECKING, Dict, List, Union

from scrapy_webarchive import __version__ as scrapy_webarchive_version
from scrapy_webarchive.cdxj import write_cdxj_index
from scrapy_webarchive.utils import (
    TIMESTAMP_DT_FORMAT,
    WARC_DT_FORMAT,
    get_formatted_dt_string,
    hash_stream,
)
from scrapy_webarchive.wacz.constants import ARCHIVE_DIR, INDEXES_DIR, WACZ_VERSION
from scrapy_webarchive.warc import WARCReader

if TYPE_CHECKING:
    from scrapy_webarchive.extensions import FilesStoreProtocol


class WaczFileCreator:
    """Handles creating WACZ archives."""

    hash_type = "sha256"
    datapackage_fname = "datapackage.json"

    def __init__(
        self, 
        store: 'FilesStoreProtocol', 
        warc_fname: str, 
        collection_name: str, 
        title: str, 
        description: str, 
        wacz_fname: Union[str, None], 
        cdxj_fname: str = "index.cdxj",
    ) -> None:
        self.store = store
        self.warc_fname = warc_fname
        self.cdxj_fname = cdxj_fname
        self.collection_name = collection_name
        self._title = title
        self._description = description
        self.wacz_fname = wacz_fname or self.get_wacz_fname()

    def create(self) -> None:
        """Create the WACZ file from the WARC and CDXJ index and save it in the configured store."""

        # Write cdxj index to a temporary file
        write_cdxj_index(output=self.cdxj_fname, inputs=[self.warc_fname])

        # Create the WACZ archive in memory
        zip_buffer = self.create_wacz_zip()

        # Cleanup the temporary files
        self.cleanup_files(self.cdxj_fname, self.warc_fname)

        # Save WACZ to the storage
        zip_buffer.seek(0)
        self.store.persist_file(path=self.wacz_fname, buf=zip_buffer, info=None)

    def create_wacz_zip(self) -> io.BytesIO:
        """Create the WACZ zip file and return the in-memory buffer."""

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED, allowZip64=True) as zip_file:
            self.write_to_zip(zip_file, self.cdxj_fname, INDEXES_DIR)
            self.write_to_zip(zip_file, self.warc_fname, ARCHIVE_DIR)
            self.write_datapackage(zip_file)

        return zip_buffer

    def write_to_zip(self, zip_file: zipfile.ZipFile, filename: str, destination: str) -> None:
        """Helper function to write a file into the ZIP archive."""

        with open(filename, "rb") as file_handle:
            zip_file.writestr(destination + os.path.basename(filename), file_handle.read())

    def cleanup_files(self, *files: str) -> None:
        """Remove files from the filesystem."""

        for file in files:
            os.remove(file)

    def get_wacz_fname(self) -> str:
        """Generate WACZ filename based on the WARC filename."""

        return f"{self.collection_name}-{get_formatted_dt_string(format=TIMESTAMP_DT_FORMAT)}.wacz"
    
    def write_datapackage(self, zip_file: zipfile.ZipFile) -> None:
        """Main function to create and write the datapackage.json."""

        package_dict = self.create_package_dict()

        with zip_file.open(ARCHIVE_DIR + self.warc_fname) as warc_fh:
            package_dict = self.update_package_metadata_from_warc(warc_fh, package_dict)

        package_dict["resources"] = self.collect_resources(zip_file)

        zip_file.writestr(self.datapackage_fname, json.dumps(package_dict, indent=2))

    def create_package_dict(self) -> Dict[str, Any]:
        """Creates the initial package dictionary."""

        dt_string = get_formatted_dt_string(format=WARC_DT_FORMAT)
        return {
            "profile": "data-package",
            "title": self.title,
            "description": self.description,
            "created": dt_string,
            "modified": dt_string,
            "wacz_version": WACZ_VERSION,
            "software": f"scrapy-webarchive/{scrapy_webarchive_version}, Scrapy/{scrapy_version}",
        }

    def update_package_metadata_from_warc(self, warc_fh: IO, package_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Updates the package dictionary with metadata from the WARC records."""

        warc_reader = WARCReader(gzip.open(warc_fh)) if self.warc_fname.endswith(".gz") else WARCReader(warc_fh)

        while True:
            warc_record = warc_reader.read_record()
            if warc_record is None:
                break
            
            if warc_record.type == "request":
                package_dict.update({
                    "mainPageUrl": warc_record.url,
                    "mainPageDate": warc_record.date,
                })
                break

        return package_dict

    def collect_resources(self, zip_file: zipfile.ZipFile) -> List[Dict[str, Any]]:
        """Collects resource information from the zip file."""

        resources = []

        for zip_entry in zip_file.infolist():
            with zip_file.open(zip_entry, "r") as stream:
                size, hash_ = hash_stream(self.hash_type, stream)

                resources.append({
                    "name": os.path.basename(zip_entry.filename).lower(),
                    "path": zip_entry.filename,
                    "hash": hash_,
                    "bytes": size,
                })

        return resources
    
    @property
    def title(self):
        return self._title or self.collection_name

    @property
    def description(self):
        return (
            self._description
            or f"This is the web archive generated by a scrapy-webarchive extension for the {self.collection_name} "
            "spider. It is mainly for scraping purposes as it does not contain any js/css data. Though it can be "
            "replayed as bare HTML if the site does not depend on JavaScript."
        )
