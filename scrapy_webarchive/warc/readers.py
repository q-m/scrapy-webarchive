from warc import WARCReader as BaseWARCReader


class WARCReader(BaseWARCReader):
    """WARC reader with compatibility for WARC version 1.0 and 1.1."""

    SUPPORTED_VERSIONS = ["1.0", "1.1"]
