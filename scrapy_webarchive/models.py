from dataclasses import dataclass
from typing import Union

from scrapy.http.response import Response

from scrapy_webarchive.utils import WEBARCHIVE_META_KEY


@dataclass
class WarcMetadata:
    """
    Encapsulates metadata about the WARC record.

    Attributes:
        action (str): The action performed ("read" or "write").
        record_id (str): The unique ID of the WARC record.
        wacz_uri (str): The URI of the WACZ file.
    """
    action: str
    record_id: str
    wacz_uri: str

    def to_dict(self) -> dict:
        """Convert the object to a dictionary for compatibility with Scrapy's meta."""

        return {
            "action": self.action,
            "record_id": self.record_id,
            "wacz_uri": self.wacz_uri,
        }
    
    @classmethod
    def from_response(cls, response: Response) -> Union["WarcMetadata", None]:
        """Create a WarcMetadata instance from a Scrapy response object."""

        if not hasattr(response, "meta"):
            return None

        warc_meta = response.meta.get(WEBARCHIVE_META_KEY)

        if not warc_meta:
            return None

        return cls(**warc_meta)
