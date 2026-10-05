# Derived from databrickslabs/sdp-meta (Databricks License, see LICENSE.txt).
# Modified: reduced to batch-only COBOL (Cobrix) ingestion; see NOTICE.
"""Run dataflow specs as batch jobs: read -> transform -> data quality -> write."""
import json
import logging

from sdp_meta.dataflow_spec import get_dataflow_specs
from sdp_meta.pipeline_readers import read_cobol

logger = logging.getLogger("sdp_meta")


class DataflowPipeline:
    """Executes one dataflow spec."""

    def __init__(self, spark, dataflow_spec):
        self.spark = spark
        self.spec = dataflow_spec

    def read(self):
        """Read the source and apply the where clauses and select expressions."""
        spec = self.spec
        if spec.sourceFormat == "cobol":
            df = read_cobol(self.spark, dict(spec.sourceDetails), dict(spec.readerConfigOptions or {}))
        else:
            df = self.spark.read.table(spec.sourceDetails["table"])
        for where_clause in spec.whereClause or []:
            df = df.where(where_clause)
        if spec.selectExp:
            df = df.selectExpr(*spec.selectExp)
        return df

    def apply_expectations(self, df):
        """Apply data quality expectations.

        expect: log violations; expect_or_fail: raise on violations;
        expect_or_drop: keep only rows passing every rule;
        expect_or_quarantine: rows matching any rule are written to the quarantine table.
        """
        spec = self.spec
        rules = json.loads(spec.dataQualityExpectations or "{}")
        warn, fail = rules.get("expect", {}), rules.get("expect_or_fail", {})
        if warn or fail:
            counts = df.selectExpr(
                *[f"count_if(({rule}) IS NOT TRUE) AS `{name}`" for name, rule in {**warn, **fail}.items()]
            ).first().asDict()
            violations = {name: count for name, count in counts.items() if count}
            if violations:
                logger.warning(f"{spec.targetTable}: expectation violations {violations}")
            failed = sorted(set(violations) & set(fail))
            if failed:
                raise ValueError(f"{spec.targetTable}: expect_or_fail violated: {failed}")
        quarantine = rules.get("expect_or_quarantine")
        if quarantine and spec.quarantineTable:
            self.write(df.where(" OR ".join(f"({rule})" for rule in quarantine.values())), spec.quarantineTable)
        drop = rules.get("expect_or_drop")
        if drop:
            df = df.where(" AND ".join(f"({rule})" for rule in drop.values()))
        return df

    def write(self, df, table):
        """Write to a managed table (Delta on Databricks)."""
        writer = df.write.mode(self.spec.writeMode)
        if self.spec.writeMode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        if self.spec.partitionColumns:
            writer = writer.partitionBy(*self.spec.partitionColumns)
        writer.saveAsTable(table)

    def run(self):
        self.write(self.apply_expectations(self.read()), self.spec.targetTable)

    @staticmethod
    def invoke_pipeline(spark, dataflowspec_table, layer, group=None):
        """Run every dataflow spec of a layer (optionally of one data flow group)."""
        for spec in get_dataflow_specs(spark, dataflowspec_table, layer, group):
            logger.info(f"Running {layer} dataflow {spec.dataFlowId} -> {spec.targetTable}")
            DataflowPipeline(spark, spec).run()
