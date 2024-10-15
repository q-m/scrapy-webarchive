import pytest

from scrapy_webarchive.cdxj import CdxjRecord


def test_cdxj_record_valid():
    # Sample valid CDXJ line
    valid_cdxj_line = "com,example)/index 20241003000000 {\"url\": \"http://example.com/index\", \"status\": \"200\"}"
    
    # Create a CdxjRecord object
    record = CdxjRecord.from_cdxline(valid_cdxj_line, wacz_file=None)
    
    # Test extracted data from the CDXJ line
    assert record.surt == "com,example)/index"
    assert record.host == "com,example"
    assert record.path == "/index"
    assert record.year == "2024"
    assert record.month == "10"
    assert record.day == "03"
    assert record.data == {"url": "http://example.com/index", "status": "200"}
    assert record.datetime == "20241003000000"
    assert str(record)


def test_cdxj_record_invalid_format():
    # Invalid CDXJ line (missing date)
    invalid_cdxj_line = "com,example)/index {\"url\": \"http://example.com/index\", \"status\": \"200\"}"
    
    # Test that the invalid line raises a ValueError
    with pytest.raises(ValueError, match=r"Invalid CDXJ line:"):
        CdxjRecord.from_cdxline(invalid_cdxj_line, wacz_file=None)


def test_cdxj_record_invalid_json_data():
    # Invalid JSON in CDXJ line
    invalid_cdxj_line = "com,example)/index 20241003000000 {\"url\": \"http://example.com/index\", \"status\": \"200\""
    
    # Test that the invalid JSON raises a ValueError
    with pytest.raises(ValueError):
        CdxjRecord.from_cdxline(invalid_cdxj_line, wacz_file=None)


def test_cdxj_record_empty_line():
    # Test that an empty line raises a ValueError
    with pytest.raises(ValueError, match=r"Invalid CDXJ line:"):
        CdxjRecord.from_cdxline('', wacz_file=None)


def test_cdxj_record_no_data_field():
    # CDXJ line with no data field
    no_data_cdxj_line = "com,example)/index 20241003000000"
    
    # Test that no data field raises a ValueError
    with pytest.raises(ValueError, match=r"Invalid CDXJ line:"):
        CdxjRecord.from_cdxline(no_data_cdxj_line, wacz_file=None)
