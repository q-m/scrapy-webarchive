import struct

CD_HEADER_SIGNATURE = b"\x50\x4B\x01\x02"
EOCD_SIGNATURE = b"\x50\x4B\x05\x06"
EOCD_RECORD_SIZE = 22
ZIP64_EOCD_SIGNATURE = b"\x50\x4B\x06\x06"
ZIP64_EOCD_LOCATOR_SIGNATURE = b"\x50\x4B\x06\x07"
ZIP64_EOCD_RECORD_SIZE = 56


def find_eocd(search_range: bytes) -> int:
    """Find the End of Central Directory (EOCD) in the byte range."""

    eocd_offset = search_range.rfind(EOCD_SIGNATURE)
    
    if eocd_offset == -1:
        raise ValueError("EOCD not found in provided search range")

    return eocd_offset


def find_zip64_eocd(locator: bytes) -> int:
    """Find the ZIP64 End of Central Directory (EOCD)."""

    if locator[:4] != ZIP64_EOCD_LOCATOR_SIGNATURE:
        raise ValueError("ZIP64 EOCD Locator not found")

    return struct.unpack("<Q", locator[8:16])[0]


def parse_eocd(eocd: bytes) -> tuple:
    """Parse the standard EOCD record."""

    cd_size = struct.unpack("<I", eocd[12:16])[0]
    cd_start = struct.unpack("<I", eocd[16:20])[0]

    return cd_start, cd_size


def parse_zip64_eocd(zip64_eocd: bytes) -> tuple:
    """Parse the ZIP64 End of Central Directory (EOCD) record."""

    cd_size = struct.unpack("<Q", zip64_eocd[40:48])[0]
    cd_start = struct.unpack("<Q", zip64_eocd[48:56])[0]

    return cd_start, cd_size


def parse_central_directory(central_directory: bytes, get_file_header_length):
    """Parse the Central Directory and return a list of file metadata."""

    entries = {}
    offset = 0

    while offset < len(central_directory):
        signature = central_directory[offset:offset + 4]
        if signature != CD_HEADER_SIGNATURE:
            break

        file_name_length = struct.unpack("<H", central_directory[offset + 28:offset + 30])[0]
        extra_field_length = struct.unpack("<H", central_directory[offset + 30:offset + 32])[0]
        compressed_size = struct.unpack("<I", central_directory[offset + 20:offset + 24])[0]
        header_offset = struct.unpack("<I", central_directory[offset + 42:offset + 46])[0]

        # Check for ZIP64 extra fields for large files
        if compressed_size == 0xFFFFFFFF:
            compressed_size = read_zip64_extra_field(central_directory, offset)

        file_name = central_directory[offset + 46:offset + 46 + file_name_length].decode("utf-8")

        entries[file_name] = {
            "header_offset": header_offset,
            "file_header_length": get_file_header_length(header_offset),
            "compressed_size": compressed_size,
        }
        offset += 46 + file_name_length + extra_field_length

    return entries


def read_zip64_extra_field(central_directory: bytes, offset: int) -> int:
    """Read the ZIP64 extra field for large file sizes or offsets."""

    extra_field_start = offset + 46
    extra_field_data = central_directory[extra_field_start:]

    # Look for the ZIP64 extra field signature
    signature, size = struct.unpack("<HH", extra_field_data[:4])

    if signature == 0x0001:  # ZIP64 Extra Field
        compressed_size = struct.unpack("<Q", extra_field_data[4:12])[0]
        return compressed_size

    raise ValueError("ZIP64 extra field not found for large file size")


def is_zip64(eocd):
    """Determine if the EOCD indicates a ZIP64 format."""

    if eocd[:4] != EOCD_SIGNATURE:
        raise ValueError("EOCD signature not found")

    total_entries_on_disk = struct.unpack('<H', eocd[10:12])[0]
    total_entries = struct.unpack('<H', eocd[8:10])[0]
    central_dir_size = struct.unpack('<I', eocd[12:16])[0]
    central_dir_offset = struct.unpack('<I', eocd[16:20])[0]

    # Check for ZIP64 overflow indicators
    return any([
        total_entries_on_disk == 0xFFFF,
        total_entries == 0xFFFF,
        central_dir_size == 0xFFFFFFFF,
        central_dir_offset == 0xFFFFFFFF,
    ])
