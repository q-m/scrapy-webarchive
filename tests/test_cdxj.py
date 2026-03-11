
from scrapy_webarchive.cdxj.models import CdxjRecord


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
    
    # Test that the invalid line raises returns None
    assert CdxjRecord.from_cdxline(invalid_cdxj_line, wacz_file=None) is None


def test_cdxj_record_invalid_json_data():
    # Invalid JSON in CDXJ line
    invalid_cdxj_line = "com,example)/index 20241003000000 {\"url\": \"http://example.com/index\", \"status\": \"200\""
    
    # Test that the invalid JSON returns None
    assert CdxjRecord.from_cdxline(invalid_cdxj_line, wacz_file=None) is None


def test_cdxj_record_empty_line():
    # Test that an empty line returns None
    assert CdxjRecord.from_cdxline('', wacz_file=None) is None


def test_cdxj_record_no_data_field():
    # CDXJ line with no data field
    no_data_cdxj_line = "com,example)/index 20241003000000"
    
    # Test that no data field returns None
    assert CdxjRecord.from_cdxline(no_data_cdxj_line, wacz_file=None) is None


def test_cdxj_record_urn_pageinfo():
    # CDXJ line starting with urn:pageinfo
    url = "urn:pageinfo:https://example.com/index"
    pageinfo_cdxj_line = url + " 20241003000000 {\"url\": \"" + url + "\" }"

    # Test that the urn:pageinfo line returns None
    assert CdxjRecord.from_cdxline(pageinfo_cdxj_line, wacz_file=None) is None
