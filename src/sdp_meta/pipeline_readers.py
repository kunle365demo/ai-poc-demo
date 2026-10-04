# Derived from databrickslabs/sdp-meta (Databricks License, see LICENSE.txt).
# Modified: reduced to batch-only COBOL (Cobrix) ingestion; see NOTICE.
"""Source readers. COBOL files are parsed by Cobrix (spark-cobol must be on the cluster)."""


def read_cobol(spark, source_details, reader_config_options):
    """Batch-read mainframe/COBOL files using the copybook that describes their layout.

    The copybook is read on the driver and passed as `copybook_contents`. Any other
    Cobrix option (record_format, encoding, segment options, ...) comes from the
    onboarding file's `bronze_reader_options`.
    """
    with open(source_details["copybook"]) as f:
        copybook = f.read()
    return (
        spark.read.format("cobol")
        .option("copybook_contents", copybook)
        .options(**reader_config_options)
        .load(source_details["path"])
    )
