import logging
import os
import shutil
import socket
import uuid
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urlparse

from cdxj_indexer.main import CDXJIndexer
from scrapy import __version__ as scrapy_version
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter


class ScrapyWarcIo:
    """Scrapy DownloaderMiddleware WARC input/output methods"""

    warc_fname = None

    def __init__(self, collection_name: str):
        self.log = logging.getLogger(
            __name__
        )  # TODO: Spider logger? Use spider logger without tightly coupling classes?
        self.collection_name = collection_name

    def write(self, response, request):
        """
        write WARC-Type: response, then WARC-Type: request records
        from Scrapy response and request

        Notes:
        1) It is customary to write the request record immediately
           after the response record to protect against a
           request-response pair spanning more than one WARC file.
        2) The WARC-Date of the response must be identical to the
           WARC-Date of the request.

        :param response  <scrapy.http.Response>
        :param request   <scrapy.http.Request>
        """

        if not hasattr(response, "status"):
            raise ValueError("Response missing HTTP status")

        if not hasattr(response, "body"):
            raise ValueError("Response missing body")

        if not hasattr(request, "method"):
            raise ValueError("Request missing method")

        if not hasattr(request, "meta"):
            raise ValueError("Request missing meta")

        if not request.meta.get("WARC-Date"):
            raise ValueError("Request missing WARC-Date")

        # If the warcfile is not set, create a new one. This only happens for the first write.
        # TODO: Improve this implementation. Should this code live here?
        if self.warc_fname is None:
            self.warc_fname = self.create_warcfile()
            self.write_warcinfo()

        record_id = self.__record_id()

        self.write_response(response, request, record_id)
        self.write_request(request, record_id)

    def write_record(
        self, url, record_type, headers, warc_headers, content_type, content, http_line
    ):
        """Write WARC record (of any type) to WARC file"""

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            http_headers = StatusAndHeaders(statusline=http_line, headers=headers)
            payload = BytesIO(bytes(content, "utf-8"))

            record = writer.create_warc_record(
                uri=url,
                record_type=record_type,
                payload=payload,
                http_headers=http_headers,
                warc_headers=warc_headers,
                warc_content_type=content_type,
            )

            writer.write_record(record)

    def write_response(self, response, request, record_id):
        warc_headers = StatusAndHeaders(
            "",
            [
                ("WARC-Type", "response"),
                ("WARC-Target-URI", response.url),
                ("WARC-Date", request.meta["WARC-Date"]),
                ("WARC-Record-ID", record_id),
            ],
            protocol="WARC/1.0",
        )

        mimetype = None  # mimetypes.guess_type(_str(body))[0]
        if mimetype:
            warc_headers.append(("WARC-Identified-Payload-Type", mimetype))

        http_line = f"HTTP/1.0 {str(response.status)}"

        headers = []
        for key in response.headers:
            val = response.headers[key]
            headers.append((key.decode(), val.decode()))

        # write response
        self.write_record(
            url=response.url,
            record_type="response",
            content=response.body.decode(),
            content_type="application/http; msgtype=response",
            headers=headers,
            warc_headers=warc_headers,
            http_line=http_line,
        )

    def write_request(self, request, concurrent_to):
        """
        write WARC-Type: request record from Scrapy response

        :param request        <scrapy.http.Request>
        :param concurrent_to  response WARC-Record-ID
        """

        warc_headers = StatusAndHeaders(
            "",
            [
                ("WARC-Type", "request"),
                ("WARC-Target-URI", request.url),
                ("WARC-Date", request.meta["WARC-Date"]),
                ("WARC-Record-ID", self.__record_id()),
                ("WARC-Concurrent-To", concurrent_to),
            ],
            protocol="WARC/1.0",
        )

        http_line = f"{request.method} {urlparse(request.url).path} HTTP/1.0"

        headers = []
        for key in request.headers:
            val = request.headers[key]
            headers.append((key.decode(), val.decode()))

        self.write_record(
            url=request.url,
            record_type="request",
            content_type="application/http; msgtype=request",
            content=request.body.decode(),
            headers=headers,
            warc_headers=warc_headers,
            http_line=http_line,
        )

    def write_warcinfo(self):
        """Write WARC-Type: warcinfo record"""

        content = {
            "software": f"Scrapy/{scrapy_version} (+https://scrapy.org)",
            "format": "WARC file version 1.0",
            "conformsTo": "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/",  # TODO: Make setting?
            "isPartOf": self.collection_name,
            "robots": "obey" if True else "ignore",  # TODO
        }

        with open(self.warc_fname, "ab") as fh:
            writer = WARCWriter(fh, gzip=True)
            record = writer.create_warcinfo_record(
                filename=self.warc_fname, info=content
            )
            writer.write_record(record)

    def create_warcfile(self):
        """
        Returns new WARC filename. WARC filename format compatible with internetarchive/draintasker warc naming #1:
        {TLA}-{timestamp}-{serial}-{fqdn}.warc.gz

        Raises IOError if WARC file exists or destination is not found.
        """

        tla = self.collection_name
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        fqdn = socket.gethostname().split(".")[0]
        warc_name = "-".join([tla, timestamp, "00000", fqdn]) + ".warc.gz"
        warc_dest = ""  # TODO

        if warc_dest and not os.path.exists(warc_dest):
            raise IOError(f"warc_dest not found: {warc_dest}")

        fname = os.path.join(warc_dest, warc_name)

        if os.path.exists(fname):
            raise IOError(f"WARC file exists: {fname}")

        self.log.info(f"New WARC file: {self.warc_fname}")
        return fname

    def create_wacz(self):
        wacz = zipfile.ZipFile("archive.wacz", "w")
        cdxj_file = "index.cdxj"

        # write index
        self.log.debug("Reading and Indexing All WARCs")
        wacz_indexer = CDXJIndexer(
            output=cdxj_file,
            inputs=[self.warc_fname],
        )
        wacz_indexer.process_all()

        wacz_index_file = zipfile.ZipInfo.from_file(
            cdxj_file, "indexes/" + os.path.basename(cdxj_file)
        )

        with wacz.open(wacz_index_file, "w") as out_fh:
            with open(cdxj_file, "rb") as in_fh:
                shutil.copyfileobj(in_fh, out_fh)

        # write archives
        self.log.debug("Writing archives...")
        for _input in [self.warc_fname]:
            archive_file = zipfile.ZipInfo.from_file(
                _input, "archive/" + os.path.basename(_input)
            )

            with wacz.open(archive_file, "w") as out_fh:
                with open(_input, "rb") as in_fh:
                    shutil.copyfileobj(in_fh, out_fh)

        wacz.close()

    @staticmethod
    def __record_id():
        """Returns WARC-Record-ID (globally unique UUID) as a string"""

        return f"<urn:uuid:{uuid.uuid1()}>"
