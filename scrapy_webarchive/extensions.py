from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.http.request import Request
from scrapy.http.response import Response
from typing_extensions import Self

from scrapy_webarchive.utils import warc_date
from scrapy_webarchive.wacz import WaczFileCreator
from scrapy_webarchive.warc import WarcFileWriter, create_warc_fname


class WaczExporter:
    """WACZ exporter extension that writes spider requests/responses as WACZ during a crawl job."""

    def __init__(self, writer: WarcFileWriter) -> None:
        self.writer = writer

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        writer = WarcFileWriter(
            warc_fname=create_warc_fname(tla=crawler.spider.name),
            collection_name=crawler.spider.name,
        )
        writer.write_warcinfo()

        o = cls(writer)
        crawler.signals.connect(o.write_warc_records, signal=signals.response_received)
        crawler.signals.connect(o.create_wacz, signal=signals.spider_closed)
        return o

    def write_warc_records(self, response: Response, request: Request) -> None:
        # TODO: Move this
        request.meta["WARC-Date"] = warc_date()

        # Write response WARC record
        response_record_id = self.writer.write_response(response, request)

        # Write request WARC record
        self.writer.write_request(request, concurrent_to=response_record_id)

    def create_wacz(self) -> None:
        wacz_creator = WaczFileCreator(warc_fname=self.writer.warc_fname)
        wacz_creator.create_wacz()
