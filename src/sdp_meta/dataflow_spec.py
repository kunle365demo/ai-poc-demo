# Derived from databrickslabs/sdp-meta (Databricks License, see LICENSE.txt).
# Modified: reduced to batch-only COBOL (Cobrix) ingestion; see NOTICE.
"""Dataflow spec: one row per (data flow, layer) in the dataflowspec table."""
from dataclasses import dataclass

SPEC_SCHEMA = (
    "dataFlowId string, dataFlowGroup string, layer string, sourceFormat string, "
    "sourceDetails map<string,string>, readerConfigOptions map<string,string>, "
    "targetTable string, quarantineTable string, selectExp array<string>, whereClause array<string>, "
    "partitionColumns array<string>, dataQualityExpectations string, writeMode string"
)


@dataclass
class DataflowSpec:
    dataFlowId: str
    dataFlowGroup: str
    layer: str  # bronze | silver
    sourceFormat: str  # cobol (bronze) | delta (silver)
    sourceDetails: dict  # cobol: path, copybook; delta: table
    readerConfigOptions: dict
    targetTable: str
    quarantineTable: str
    selectExp: list
    whereClause: list
    partitionColumns: list
    dataQualityExpectations: str  # JSON
    writeMode: str  # overwrite | append


def get_dataflow_specs(spark, dataflowspec_table, layer, group=None):
    """Read the specs for a layer, optionally restricted to one data flow group."""
    df = spark.read.table(dataflowspec_table).where(f"layer = '{layer}'")
    if group:
        df = df.where(f"dataFlowGroup = '{group}'")
    return [DataflowSpec(**row.asDict()) for row in df.collect()]
