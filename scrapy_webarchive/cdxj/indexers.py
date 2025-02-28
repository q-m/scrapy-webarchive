from __future__ import annotations

from cdxj_indexer.main import CDXJIndexer
from typing_extensions import List


def write_cdxj_index(output: str, inputs: List[str]) -> str:
    """Generates a CDXJ index from a list of input WARC files and writes the index to an output file."""

    wacz_indexer = CDXJIndexer(output=output, inputs=inputs)
    wacz_indexer.process_all()
    return output
