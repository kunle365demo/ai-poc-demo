import json
import os
import unittest
from unittest.mock import MagicMock

from sdp_meta.dataflow_pipeline import DataflowPipeline
from sdp_meta.dataflow_spec import DataflowSpec

COPYBOOK = os.path.join(os.path.dirname(__file__), "..", "examples", "conf", "copybooks", "customers.cpy")


def spec(**overrides):
    fields = dict(
        dataFlowId="100", dataFlowGroup="A1", layer="bronze", sourceFormat="cobol",
        sourceDetails={"path": "/data/customers", "copybook": COPYBOOK},
        readerConfigOptions={"record_format": "F"}, targetTable="db.customers_bronze",
        quarantineTable=None, selectExp=None, whereClause=None, partitionColumns=None,
        dataQualityExpectations=None, writeMode="overwrite",
    )
    return DataflowSpec(**{**fields, **overrides})


class PipelineTests(unittest.TestCase):
    def test_bronze_reads_cobol_with_cobrix(self):
        spark = MagicMock()
        DataflowPipeline(spark, spec()).read()
        spark.read.format.assert_called_once_with("cobol")
        reader = spark.read.format.return_value
        option, contents = reader.option.call_args.args
        self.assertEqual(option, "copybook_contents")
        self.assertIn("CUSTOMER-RECORD", contents)
        reader.option.return_value.options.assert_called_once_with(record_format="F")
        reader.option.return_value.options.return_value.load.assert_called_once_with("/data/customers")
        spark.readStream.assert_not_called()

    def test_silver_reads_table_and_transforms(self):
        spark = MagicMock()
        silver = spec(layer="silver", sourceFormat="delta", sourceDetails={"table": "db.customers_bronze"},
                      whereClause=["STATE IS NOT NULL"], selectExp=["CUSTOMER_ID AS id"])
        DataflowPipeline(spark, silver).read()
        spark.read.table.assert_called_once_with("db.customers_bronze")
        filtered = spark.read.table.return_value.where
        filtered.assert_called_once_with("STATE IS NOT NULL")
        filtered.return_value.selectExpr.assert_called_once_with("CUSTOMER_ID AS id")

    def test_expectations_drop_quarantine_and_warn(self):
        rules = {"expect": {"positive": "BALANCE >= 0"},
                 "expect_or_drop": {"valid_id": "ID IS NOT NULL", "valid_state": "STATE IS NOT NULL"},
                 "expect_or_quarantine": {"bad_id": "ID IS NULL"}}
        df = MagicMock()
        df.selectExpr.return_value.first.return_value.asDict.return_value = {"positive": 2}
        pipeline = DataflowPipeline(MagicMock(), spec(dataQualityExpectations=json.dumps(rules),
                                                      quarantineTable="db.customers_quarantine"))
        result = pipeline.apply_expectations(df)
        df.selectExpr.assert_called_once_with("count_if((BALANCE >= 0) IS NOT TRUE) AS `positive`")
        df.where.assert_any_call("(ID IS NULL)")
        df.where.assert_called_with("(ID IS NOT NULL) AND (STATE IS NOT NULL)")
        self.assertIs(result, df.where.return_value)
        saved = df.where.return_value.write.mode.return_value.option.return_value.saveAsTable
        saved.assert_called_once_with("db.customers_quarantine")

    def test_expect_or_fail_raises(self):
        df = MagicMock()
        df.selectExpr.return_value.first.return_value.asDict.return_value = {"valid_id": 1}
        rules = json.dumps({"expect_or_fail": {"valid_id": "ID IS NOT NULL"}})
        with self.assertRaises(ValueError):
            DataflowPipeline(MagicMock(), spec(dataQualityExpectations=rules)).apply_expectations(df)

    def test_write_modes_and_partitioning(self):
        df = MagicMock()
        DataflowPipeline(MagicMock(), spec(writeMode="append", partitionColumns=["STATE"])).write(df, "db.t")
        df.write.mode.assert_called_once_with("append")
        df.write.mode.return_value.option.assert_not_called()
        df.write.mode.return_value.partitionBy.assert_called_once_with("STATE")
        df.write.mode.return_value.partitionBy.return_value.saveAsTable.assert_called_once_with("db.t")

    def test_invoke_pipeline_runs_each_spec_of_the_layer(self):
        spark = MagicMock()
        row = MagicMock()
        row.asDict.return_value = vars(spec())
        table = spark.read.table.return_value
        table.where.return_value.where.return_value.collect.return_value = [row]
        DataflowPipeline.invoke_pipeline(spark, "db.dataflowspec", "bronze", "A1")
        spark.read.table.assert_called_once_with("db.dataflowspec")
        table.where.assert_called_once_with("layer = 'bronze'")
        table.where.return_value.where.assert_called_once_with("dataFlowGroup = 'A1'")
        spark.read.format.assert_called_once_with("cobol")


if __name__ == "__main__":
    unittest.main()
