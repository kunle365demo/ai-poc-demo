# Derived from databrickslabs/sdp-meta (Databricks License, see LICENSE.txt).
# Modified: reduced to batch-only COBOL ingestion; see NOTICE.
"""Source readers. COBOL files are decoded in Python so the job can run on serverless compute."""
import os

from sdp_meta.cobol_parser import decode_records, parse_copybook


def read_cobol(spark, source_details, reader_config_options):
    """Batch-read fixed-length COBOL files using the copybook that describes their layout.

    Files are read and decoded on the driver, so they must fit in its memory.
    `bronze_reader_options`: `encoding` is a Python codec name (default cp037, i.e. EBCDIC US;
    use `ascii` for ASCII files).
    """
    with open(source_details["copybook"]) as f:
        fields, record_length = parse_copybook(f.read())
    path = source_details["path"]
    if os.path.isdir(path):
        files = sorted(os.path.join(path, name) for name in os.listdir(path) if name[0] not in "._")
    else:
        files = [path]
    rows = []
    for file in files:
        with open(file, "rb") as f:
            rows += decode_records(f.read(), fields, record_length, reader_config_options.get("encoding", "cp037"))
    return spark.createDataFrame(rows, ", ".join(f"`{f.name}` {f.spark_type}" for f in fields))
