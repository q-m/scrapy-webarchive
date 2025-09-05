import pytest


@pytest.fixture
def warc_example():
    return b"\
WARC/1.0\r\n\
Content-Length: 10\r\n\
WARC-Date: 2024-02-10T16:15:52Z\r\n\
Content-Type: application/http; msgtype=request\r\n\
WARC-Type: request\r\n\
WARC-Record-ID: <urn:uuid:80fb9262-5402-11e1-8206-545200690126>\r\n\
WARC-Target-URI: http://example.com/\r\n\
\r\n\
Helloworld\
\r\n\r\n\
"


def pytest_configure(config):
    # install the reactor explicitly
    from twisted.internet import reactor  # noqa: F401
