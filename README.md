# ai-poc-demo — sdp-meta, stripped down for COBOL ingestion

A minimal, **batch-only** derivative of [databrickslabs/sdp-meta](https://github.com/databrickslabs/sdp-meta).
It keeps the metadata-driven idea — describe your data flows in an onboarding file, let generic code
run them — and supports exactly one source: **fixed-length COBOL / mainframe files**, decoded from
their copybook by a small pure-Python parser so the job runs on **serverless compute**.

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
| `run --layer bronze` | Decodes the COBOL files using the copybook, applies data quality expectations, writes the bronze table. |
| `run --layer silver` | Reads the bronze table, applies `where_clause` / `select_exp`, writes the silver table. |

The code is ~350 lines in `src/sdp_meta/`:

| File | Purpose |
|------|---------|
| `onboard_dataflowspec.py` | Onboarding file → dataflow specs |
| `dataflow_spec.py` | Spec dataclass, spec table schema, spec lookup |
| `cobol_parser.py` | Copybook parser and record decoder (pure Python) |
| `pipeline_readers.py` | Reads COBOL files into a DataFrame |
| `dataflow_pipeline.py` | Read → transform → expectations → write |
| `__main__.py` | `sdp_meta onboard` / `sdp_meta run` entry point |

### COBOL support and limits

Upstream sdp-meta runs inside Lakeflow Spark Declarative Pipelines; this version is a plain batch job that
writes tables with `saveAsTable`. [Cobrix](https://github.com/AbsaOSS/cobrix) is not used because it is a JVM
library, which neither pipelines nor serverless compute can load.

| Supported | Not supported |
|-----------|---------------|
| Fixed-length records | Variable-length / multi-segment records |
| `PIC X` / `PIC A` text | `OCCURS`, `REDEFINES` |
| `PIC [S]9[V9]` as DISPLAY (zoned), `COMP-3` (packed), `COMP` / `BINARY` | `COMP-1`, `COMP-2`, separate signs |
| Nested groups (flattened), `FILLER`, EBCDIC or ASCII | Files larger than driver memory |

Files are read and decoded on the driver, so the source path must be a local-style path such as
`/Volumes/...`. For anything in the right-hand column, use Cobrix on classic compute instead.

## Onboarding file

See [`examples/conf/onboarding.json`](examples/conf/onboarding.json). Keys ending in `_{env}` are picked by `--env`.

| Key | Required | Description |
|-----|----------|-------------|
| `data_flow_id`, `data_flow_group` | yes | Identity of the flow; jobs run one group at a time |
| `source_format` | yes | Must be `cobol` |
| `source_details.source_path_{env}` | yes | File or directory of COBOL data, e.g. `/Volumes/...` |
| `source_details.copybook_path_{env}` | yes | Copybook describing the record layout |
| `bronze_reader_options` | no | `encoding`: Python codec of the text fields, default `cp037` (EBCDIC US); e.g. `cp500`, `cp1047`, `ascii` |
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
   CREATE SCHEMA IF NOT EXISTS fip_poc.fip_poc_sc;
   CREATE VOLUME IF NOT EXISTS fip_poc.fip_poc_sc.landing;
   ```
   ```sh
   databricks fs cp examples/data/customers.dat dbfs:/Volumes/fip_poc/fip_poc_sc/landing/customers/customers.dat
   ```
2. Adjust catalog/schema names in `examples/conf/onboarding.json` and `dataflowspec_table` in `databricks.yml`.
3. Deploy and run:
   ```sh
   databricks bundle deploy
   databricks bundle run cobol_ingestion
   ```

Result: `customers_bronze` (3 rows), `customers_quarantine` (the record with id 0) and `customers` (silver).

Without the bundle, install the wheel and call it from a notebook:

```python
from sdp_meta.onboard_dataflowspec import onboard_dataflow_specs
from sdp_meta.dataflow_pipeline import DataflowPipeline

onboard_dataflow_specs(spark, "/Workspace/.../conf/onboarding.json", "dev", "fip_poc.fip_poc_sc.dataflowspec")
DataflowPipeline.invoke_pipeline(spark, "fip_poc.fip_poc_sc.dataflowspec", "bronze", group="A1")
DataflowPipeline.invoke_pipeline(spark, "fip_poc.fip_poc_sc.dataflowspec", "silver", group="A1")
```

## Tests

```sh
PYTHONPATH=src python -m unittest discover -s tests
```

The parser tests decode real bytes (including the sample file); Spark itself is mocked.

## License

Derivative work of sdp-meta, distributed under the [Databricks License](LICENSE.txt); see [NOTICE](NOTICE).
