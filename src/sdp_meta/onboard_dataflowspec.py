# Derived from databrickslabs/sdp-meta (Databricks License, see LICENSE.txt).
# Modified: reduced to batch-only COBOL (Cobrix) ingestion; see NOTICE.
"""Onboarding: turn a JSON onboarding file into rows of the dataflowspec table."""
import json
import os

from sdp_meta.dataflow_spec import SPEC_SCHEMA


def _load_json(path):
    with open(path) as f:
        return json.load(f)


def _table_name(row, env, layer, quarantine=False):
    """Build [catalog.]database.table for a layer's target or quarantine table."""
    table = row.get(f"{layer}_quarantine_table" if quarantine else f"{layer}_table")
    if not table:
        return None
    suffix = f"_quarantine_{env}" if quarantine else f"_{env}"
    parts = [row.get(f"{layer}_catalog{suffix}"), row[f"{layer}_database{suffix}"], table]
    return ".".join(p for p in parts if p)


def _layer_spec(row, env, layer, resolve):
    """Spec fields shared by the bronze and silver layers."""
    dqe_path = row.get(f"{layer}_data_quality_expectations_json_{env}")
    return {
        "dataFlowId": str(row["data_flow_id"]),
        "dataFlowGroup": row["data_flow_group"],
        "layer": layer,
        "readerConfigOptions": None,
        "selectExp": None,
        "whereClause": None,
        "targetTable": _table_name(row, env, layer),
        "quarantineTable": _table_name(row, env, layer, quarantine=True),
        "partitionColumns": row.get(f"{layer}_partition_columns"),
        "dataQualityExpectations": json.dumps(_load_json(resolve(dqe_path))) if dqe_path else None,
        "writeMode": row.get(f"{layer}_write_mode", "overwrite"),
    }


def build_specs(onboarding_file_path, env):
    """Parse the onboarding file into a list of spec dicts (bronze, plus silver when configured).

    Relative config paths (copybook, expectations, silver transformations) are resolved
    against the onboarding file's directory.
    """
    base_dir = os.path.dirname(os.path.abspath(onboarding_file_path))

    def resolve(path):
        return os.path.join(base_dir, path)

    specs = []
    for row in _load_json(onboarding_file_path):
        if row["source_format"] != "cobol":
            raise ValueError(f"Unsupported source_format '{row['source_format']}': only 'cobol' is supported")
        source = row["source_details"]
        bronze = _layer_spec(row, env, "bronze", resolve)
        bronze.update(
            sourceFormat="cobol",
            sourceDetails={
                "path": source[f"source_path_{env}"],
                "copybook": resolve(source[f"copybook_path_{env}"]),
            },
            readerConfigOptions={
                k: str(v).lower() if isinstance(v, bool) else str(v)
                for k, v in row.get("bronze_reader_options", {}).items()
            },
        )
        specs.append(bronze)
        if row.get("silver_table"):
            transformations_path = row.get(f"silver_transformation_json_{env}")
            transformations = _load_json(resolve(transformations_path)) if transformations_path else []
            transformation = next((t for t in transformations if t["target_table"] == row["silver_table"]), {})
            silver = _layer_spec(row, env, "silver", resolve)
            silver.update(
                sourceFormat="delta",
                sourceDetails={"table": bronze["targetTable"]},
                selectExp=transformation.get("select_exp"),
                whereClause=transformation.get("where_clause"),
            )
            specs.append(silver)
    return specs


def onboard_dataflow_specs(spark, onboarding_file_path, env, dataflowspec_table):
    """Replace the dataflowspec table with the specs built from the onboarding file."""
    specs = build_specs(onboarding_file_path, env)
    (
        spark.createDataFrame(specs, SPEC_SCHEMA)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dataflowspec_table)
    )
    return specs
