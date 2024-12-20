import struct

import pytest

from scrapy_webarchive.wacz import zip_utils


def test_find_eocd():
    file_size = 100000
    data = b"A" * (file_size - 22) + zip_utils.EOCD_SIGNATURE + b"B" * 18
    eocd_offset = zip_utils.find_eocd(data)
    assert eocd_offset == file_size - 22


def test_find_eocd_not_found():
    file_size = 100000
    data = b"A" * file_size

    with pytest.raises(ValueError, match="EOCD not found in provided search range"):
        zip_utils.find_eocd(data)


def test_find_zip64_eocd():
    locator = zip_utils.ZIP64_EOCD_LOCATOR_SIGNATURE + b"\x00" * 4 + struct.pack("<Q", 12345)
    zip64_offset = zip_utils.find_zip64_eocd(locator)
    assert zip64_offset == 12345


def test_find_zip64_eocd_not_found():
    locator = b"A" * 20
    
    with pytest.raises(ValueError, match="ZIP64 EOCD Locator not found"):
        zip_utils.find_zip64_eocd(locator)


def test_parse_eocd():
    eocd = b"A" * 12 + struct.pack("<I", 4000) + struct.pack("<I", 8000)
    cd_start, cd_size = zip_utils.parse_eocd(eocd)
    assert cd_start == 8000
    assert cd_size == 4000


def test_parse_zip64_eocd():
    zip64_eocd = b"A" * 40 + struct.pack("<Q", 5000) + struct.pack("<Q", 10000)
    cd_start, cd_size = zip_utils.parse_zip64_eocd(zip64_eocd)
    assert cd_start == 10000
    assert cd_size == 5000


def test_is_zip64():
    eocd = zip_utils.EOCD_SIGNATURE + b"A" * 4 + struct.pack("<H", 0xFFFF) + struct.pack("<H", 0xFFFF) \
        + struct.pack("<I", 0xFFFFFFFF) + struct.pack("<I", 0xFFFFFFFF)
    assert zip_utils.is_zip64(eocd)

    eocd = zip_utils.EOCD_SIGNATURE + b"A" * 4 + struct.pack("<H", 100) + struct.pack("<H", 100) \
        + struct.pack("<I", 4000) + struct.pack("<I", 8000)
    assert not zip_utils.is_zip64(eocd)


def test_parse_central_directory():
    central_directory = (
        zip_utils.CD_HEADER_SIGNATURE  # 4 bytes
        + b"\x14\x00"                  # Version made by
        + b"\x14\x00"                  # Version needed to extract
        + b"\x00\x00"                  # General purpose bit flag
        + b"\x00\x00"                  # Compression method
        + b"\x00\x00"                  # File modification time
        + b"\x00\x00"                  # File modification date
        + b"A" * 4                     # CRC-32
        + struct.pack("<I", 4000)      # Compressed size
        + struct.pack("<I", 4000)      # Uncompressed size
        + struct.pack("<H", 5)         # Filename length
        + struct.pack("<H", 5)         # Extra field length
        + struct.pack("<H", 0)         # File comment length
        + struct.pack("<H", 0)         # Disk number start
        + struct.pack("<H", 0)         # Internal file attributes
        + struct.pack("<I", 0)         # External file attributes
        + struct.pack("<I", 1234)      # Relative offset of local header
        + b"hello"                     # Filename (5 bytes)
        + b"extra"                     # Extra field (5 bytes)
    )
    def mock_get_file_header_length(*args, **kwargs):
        return 46

    entries = zip_utils.parse_central_directory(central_directory, mock_get_file_header_length)

    assert entries["hello"] == {
        "header_offset": 1234,
        "file_header_length": 46,
        "compressed_size": 4000,
    }


def test_read_zip64_extra_field():
    central_directory = b"A" * 46 + struct.pack("<HH", 0x0001, 16) + struct.pack("<Q", 5000)
    compressed_size = zip_utils.read_zip64_extra_field(central_directory, 0)
    assert compressed_size == 5000

    with pytest.raises(ValueError, match="ZIP64 extra field not found"):
        central_directory = b"A" * 46 + struct.pack("<HH", 0x0002, 16)
        zip_utils.read_zip64_extra_field(central_directory, 0)
