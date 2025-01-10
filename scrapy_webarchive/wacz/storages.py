import gzip
import os
import struct
import zipfile
from abc import ABC, abstractmethod
from io import BytesIO
from urllib.parse import unquote, urlparse

from scrapy.exceptions import NotConfigured
from scrapy.utils.boto import is_botocore_available

from scrapy_webarchive.exceptions import UnsupportedURIException
from scrapy_webarchive.wacz import zip_utils


class ZipStorageHandler(ABC):
    """Abstract Base Class for ZIP storage handlers."""

    @abstractmethod
    def fetch_file(self, file_name: str) -> bytes:
        """Fetch the entire content of a file."""

        pass

    @abstractmethod
    def fetch_file_part(self, file_name: str, offset: int, size: int) -> bytes:
        """Fetch a specific part of a file."""

        pass

    @property
    @abstractmethod
    def zip_exists(self) -> bool:
        """Check if the ZIP file exists."""

        pass


class LocalZipStorageHandler(ZipStorageHandler):
    """Handles ZIP files stored locally."""

    def __init__(self, uri: str):
        """Initialize the handler with a local ZIP file."""

        self.uri = uri
        parsed_uri = urlparse(self.uri)
        self.file_path = unquote(parsed_uri.path)

        if not self.zip_exists:
            raise FileNotFoundError(f"ZIP file not found at {uri}")

        self.zip_file = zipfile.ZipFile(self.file_path)

    def fetch_file_part(self, file_name: str, offset: int, size: int) -> bytes:
        if file_name not in self.zip_file.namelist():
            raise FileNotFoundError(f"File {file_name} not found in ZIP archive")

        with self.zip_file.open(file_name) as file_obj:
            file_obj.seek(offset)

            if file_name.endswith(".gz"):
                with gzip.open(file_obj) as gz_file:
                    return gz_file.read()

            return file_obj.read()

    def fetch_file(self, file_name: str) -> bytes:
        if file_name not in self.zip_file.namelist():
            raise FileNotFoundError(f"File {file_name} not found in ZIP archive")

        with self.zip_file.open(file_name) as file_obj:
            if file_name.endswith(".gz"):
                with gzip.open(file_obj) as gz_file:
                    return gz_file.read()
            return file_obj.read()

    @property
    def zip_exists(self) -> bool:
        return os.path.exists(self.file_path)


class RemoteZipStorageHandler(ZipStorageHandler):
    """Abstract base class for remote ZIP file handlers."""

    @abstractmethod
    def get_object(self, range_bytes: str) -> bytes:
        """Fetch an object (or part of it) from remote storage."""

        pass

    @abstractmethod
    def get_file_info(self) -> dict:
        """Get metadata about the remote file."""

        pass

    def _find_eocd(self, file_size: int) -> int:
        """Find the End of Central Directory (EOCD) in the ZIP file."""

        search_offset = max(0, file_size - 65536)
        range_bytes = f"bytes={search_offset}-{file_size - 1}"
        search_range = self.get_object(range_bytes)
        return zip_utils.find_eocd(search_range) + search_offset

    def _find_zip64_eocd(self, eocd_offset: int) -> int:
        """Find the ZIP64 End of Central Directory (EOCD)."""

        locator_offset = eocd_offset - 20
        range_bytes = f"bytes={locator_offset}-{eocd_offset - 1}"
        locator = self.get_object(range_bytes)
        return zip_utils.find_zip64_eocd(locator) + locator_offset

    def _get_file_header_length(self, offset: int) -> int:
        """Retrieve the length of a file header from the ZIP file."""

        range_bytes = f"bytes={offset}-{offset + 29}"
        local_header = self.get_object(range_bytes)

        if local_header[:4] != b"\x50\x4b\x03\x04":
            raise ValueError("Invalid Local File Header signature")

        file_name_length, extra_field_length = struct.unpack("<HH", local_header[26:30])
        return 30 + file_name_length + extra_field_length

    def _get_zip_metadata(self) -> dict:
        """Retrieve metadata from the ZIP file, including central directory information."""

        file_size = self.get_file_info()["ContentLength"]
        eocd_offset = self._find_eocd(file_size)
        range_bytes = f"bytes={eocd_offset}-{eocd_offset + zip_utils.EOCD_RECORD_SIZE - 1}"
        eocd = self.get_object(range_bytes)

        if zip_utils.is_zip64(eocd):
            zip64_eocd_offset = self._find_zip64_eocd(eocd_offset)
            range_bytes = f"bytes={zip64_eocd_offset}-{zip64_eocd_offset + zip_utils.ZIP64_EOCD_RECORD_SIZE - 1}"
            zip64_eocd = self.get_object(range_bytes)
            cd_start, cd_size = zip_utils.parse_zip64_eocd(zip64_eocd)
        else:
            cd_start, cd_size = zip_utils.parse_eocd(eocd)

        range_bytes = f"bytes={cd_start}-{cd_start + cd_size - 1}"
        central_directory = self.get_object(range_bytes)

        return zip_utils.parse_central_directory(central_directory, self._get_file_header_length)


class S3ZipStorageHandler(RemoteZipStorageHandler):
    """Handles ZIP files stored in Amazon S3."""
    
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None
    AWS_SESSION_TOKEN = None
    AWS_ENDPOINT_URL = None
    AWS_REGION_NAME = None
    AWS_USE_SSL = None
    AWS_VERIFY = None

    def __init__(self, uri: str):
        if not is_botocore_available():
            raise NotConfigured("missing botocore library")

        parse_result = urlparse(uri, allow_fragments=False)
        if parse_result.scheme != "s3":
            raise ValueError(f"Incorrect URI scheme in {uri}, expected 's3'")

        self.bucket = parse_result.netloc
        self.path = parse_result.path.lstrip("/")
        self.s3_client = self._initialize_s3_client()
        self.zip_metadata = self._get_zip_metadata()

    def _initialize_s3_client(self):
        import botocore.session
        session = botocore.session.get_session()
        return session.create_client(
            "s3",
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            aws_session_token=self.AWS_SESSION_TOKEN,
            endpoint_url=self.AWS_ENDPOINT_URL,
            region_name=self.AWS_REGION_NAME,
            use_ssl=self.AWS_USE_SSL,
            verify=self.AWS_VERIFY,
        )

    def get_object(self, range_bytes: str) -> bytes:
        response = self.s3_client.get_object(Bucket=self.bucket, Key=self.path, Range=range_bytes)
        return response["Body"].read()

    def get_file_info(self) -> dict:
        return self.s3_client.head_object(Bucket=self.bucket, Key=self.path)

    def fetch_file_part(self, file_name: str, offset: int, size: int) -> bytes:
        metadata = self.zip_metadata[file_name]
        range_bytes = self._calculate_range(metadata, offset, size)
        data = self.get_object(range_bytes)

        return gzip.open(BytesIO(data)).read() if file_name.endswith(".gz") else data

    def fetch_file(self, file_name: str) -> bytes:
        metadata = self.zip_metadata[file_name]
        range_bytes = self._calculate_range(metadata, 0, metadata["compressed_size"])
        return self.get_object(range_bytes)

    def _calculate_range(self, metadata: dict, offset: int, size: int) -> str:
        start = metadata["header_offset"] + metadata["file_header_length"] + offset
        end = start + size - 1
        return f"bytes={start}-{end}"

    @property
    def zip_exists(self) -> bool:
        return bool(self.get_file_info())


class ZipStorageHandlerFactory:
    @staticmethod
    def get_handler(uri: str, settings) -> ZipStorageHandler:
        if uri.startswith('s3://'):
            handler = S3ZipStorageHandler
            handler.AWS_ACCESS_KEY_ID = settings["AWS_ACCESS_KEY_ID"]
            handler.AWS_SECRET_ACCESS_KEY = settings["AWS_SECRET_ACCESS_KEY"]
            handler.AWS_SESSION_TOKEN = settings["AWS_SESSION_TOKEN"]
            handler.AWS_ENDPOINT_URL = settings["AWS_ENDPOINT_URL"]
            handler.AWS_REGION_NAME = settings["AWS_REGION_NAME"]
            handler.AWS_USE_SSL = settings["AWS_USE_SSL"]
            handler.AWS_VERIFY = settings["AWS_VERIFY"]
            return handler(uri)
        elif uri.startswith('file://'):
            return LocalZipStorageHandler(uri)
        else:
            raise UnsupportedURIException(f"URI scheme not supported: {uri}")
