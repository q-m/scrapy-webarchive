import socket

from freezegun import freeze_time

from scrapy_webarchive.warc import generate_warc_fname


@freeze_time("2024-10-04 08:27:11")
def test_generate_warc_fname(monkeypatch):
    prefix = "rec"

    # Use pytest's monkeypatch to mock the return value of socket.gethostname
    monkeypatch.setattr(socket, "gethostname", lambda: "example.local")

    # Call the function
    warc_fname = generate_warc_fname(prefix)

    # Assert the result matches the expected filename
    assert warc_fname == "rec-20241004082711-00000-example.warc.gz"
