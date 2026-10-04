# ai-poc-demo — sdp-meta, stripped down for COBOL ingestion

A minimal, **batch-only** derivative of [databrickslabs/sdp-meta](https://github.com/databrickslabs/sdp-meta).
It keeps the metadata-driven idea — describe your data flows in an onboarding file, let generic code
run them — and supports exactly one source: **COBOL / mainframe files parsed with
[Cobrix](https://github.com/AbsaOSS/cobrix)** (`spark-cobol`).

Everything related to streaming, Kafka, Event Hubs, Auto Loader, CDC/SCD, append flows, sinks,
snapshots, the CLI, the app, MCP, demos and backwards compatibility has been removed.

## How it works

```
onboarding.json ──onboard──▶ dataflowspec table ──run bronze──▶ bronze table (+ quarantine)
                                                └─run silver──▶ silver table
```

| Step | What happens |
|------|--------------|
| `onboard` | Parses the onboarding file into one spec row per data flow and layer, and overwrites the dataflowspec table. |
| `run --layer bronze` | `spark.read.format("cobol")` with the copybook, applies data quality expectations, writes the bronze table. |
| `run --layer silver` | Reads the bronze table, applies `where_clause` / `select_exp`, writes the silver table. |

The code is ~270 lines in `src/sdp_meta/`:

| File | Purpose |
|------|---------|
| `onboard_dataflowspec.py` | Onboarding file → dataflow specs |
| `dataflow_spec.py` | Spec dataclass, spec table schema, spec lookup |
| `pipeline_readers.py` | The Cobrix reader |
| `dataflow_pipeline.py` | Read → transform → expectations → write |
| `__main__.py` | `sdp_meta onboard` / `sdp_meta run` entry point |

### Why a job and not a declarative pipeline

Upstream sdp-meta runs inside Lakeflow Spark Declarative Pipelines. Cobrix is a JVM (Scala) library, and
Databricks [does not support JVM libraries in pipelines](https://docs.databricks.com/aws/en/ldp/developer/external-dependencies).
So this version runs as a regular batch **job on classic compute** with Cobrix attached as a Maven library,
and writes tables with plain `saveAsTable`. Use dedicated (single-user) access mode; serverless is not supported.

## Onboarding file

See [`examples/conf/onboarding.json`](examples/conf/onboarding.json). Keys ending in `_{env}` are picked by `--env`.

| Key | Required | Description |
|-----|----------|-------------|
| `data_flow_id`, `data_flow_group` | yes | Identity of the flow; jobs run one group at a time |
| `source_format` | yes | Must be `cobol` |
| `source_details.source_path_{env}` | yes | File or directory of COBOL data (Spark-readable, e.g. `/Volumes/...`) |
| `source_details.copybook_path_{env}` | yes | Copybook describing the record layout |
| `bronze_reader_options` | no | Any [Cobrix option](https://github.com/AbsaOSS/cobrix#spark-sql-application-options) (`record_format`, `encoding`, `segment_field`, ...) |
| `bronze_catalog_{env}`, `bronze_database_{env}`, `bronze_table` | database + table | Bronze target |
| `bronze_data_quality_expectations_json_{env}` | no | Expectations file (below) |
| `bronze_catalog_quarantine_{env}`, `bronze_database_quarantine_{env}`, `bronze_quarantine_table` | no | Where quarantined rows go |
| `bronze_partition_columns`, `bronze_write_mode` | no | Partition columns; `overwrite` (default) or `append` |
| `silver_catalog_{env}`, `silver_database_{env}`, `silver_table` | no | Silver target; omit `silver_table` for bronze-only |
| `silver_transformation_json_{env}` | no | File with `select_exp` / `where_clause` per `target_table` |
| `silver_data_quality_expectations_json_{env}`, `silver_partition_columns`, `silver_write_mode`, silver quarantine keys | no | Same as bronze |

The copybook, expectations and silver transformation paths may be relative to the onboarding file.
Target schemas must already exist.

### Data quality expectations

```json
{
  "expect":               {"non_negative_balance": "BALANCE >= 0"},
  "expect_or_drop":       {"valid_id": "CUSTOMER_ID IS NOT NULL"},
  "expect_or_fail":       {"has_state": "STATE IS NOT NULL"},
  "expect_or_quarantine": {"invalid_id": "CUSTOMER_ID IS NULL"}
}
```

`expect` logs violation counts, `expect_or_drop` keeps only rows passing every rule, `expect_or_fail`
fails the run, and rows matching any `expect_or_quarantine` rule are written to the quarantine table.

## Run the example

1. Create the schema and a volume, then upload [`examples/data/customers.dat`](examples/data/customers.dat)
   (4 fixed-length EBCDIC records matching [`customers.cpy`](examples/conf/copybooks/customers.cpy)):
   ```sql
   CREATE SCHEMA IF NOT EXISTS main.cobol_demo;
   CREATE VOLUME IF NOT EXISTS main.cobol_demo.landing;
   ```
   ```sh
   databricks fs cp examples/data/customers.dat dbfs:/Volumes/main/cobol_demo/landing/customers/customers.dat
   ```
2. Adjust catalog/schema names in `examples/conf/onboarding.json` and the variables in `databricks.yml`
   (`node_type_id` is cloud-specific; `spark_version` and `cobrix_maven` must use the same Scala version).
3. Deploy and run:
   ```sh
   databricks bundle deploy
   databricks bundle run cobol_ingestion
   ```

Result: `customers_bronze` (3 rows), `customers_quarantine` (the record with id 0) and `customers` (silver).

Without the bundle, install the wheel and Cobrix on a cluster and call it from a notebook:

```python
from sdp_meta.onboard_dataflowspec import onboard_dataflow_specs
from sdp_meta.dataflow_pipeline import DataflowPipeline

onboard_dataflow_specs(spark, "/Workspace/.../conf/onboarding.json", "dev", "main.cobol_demo.dataflowspec")
DataflowPipeline.invoke_pipeline(spark, "main.cobol_demo.dataflowspec", "bronze", group="A1")
DataflowPipeline.invoke_pipeline(spark, "main.cobol_demo.dataflowspec", "silver", group="A1")
```

## Tests

```sh
PYTHONPATH=src python -m unittest discover -s tests
```

The tests use mocks and need neither Spark nor Cobrix.

## License

Derivative work of sdp-meta, distributed under the [Databricks License](LICENSE.txt); see [NOTICE](NOTICE).
