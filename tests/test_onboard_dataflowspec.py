import os
import unittest
from unittest.mock import MagicMock

from sdp_meta.dataflow_spec import SPEC_SCHEMA, DataflowSpec
from sdp_meta.onboard_dataflowspec import build_specs, onboard_dataflow_specs

CONF = os.path.join(os.path.dirname(__file__), "..", "examples", "conf")
ONBOARDING = os.path.join(CONF, "onboarding.json")


class OnboardTests(unittest.TestCase):
    def test_build_specs_from_example(self):
        bronze, silver = build_specs(ONBOARDING, "dev")
        DataflowSpec(**bronze)  # each spec has exactly the fields of the spec table
        DataflowSpec(**silver)

        self.assertEqual(bronze["layer"], "bronze")
        self.assertEqual(bronze["sourceFormat"], "cobol")
        self.assertEqual(bronze["sourceDetails"]["path"], "/Volumes/fip_poc/fip_poc_sc/landing/customers")
        self.assertTrue(os.path.isfile(bronze["sourceDetails"]["copybook"]))
        self.assertEqual(bronze["readerConfigOptions"], {"encoding": "cp037"})
        self.assertEqual(bronze["targetTable"], "fip_poc.fip_poc_sc.customers_bronze")
        self.assertEqual(bronze["quarantineTable"], "fip_poc.fip_poc_sc.customers_quarantine")
        self.assertIn("expect_or_drop", bronze["dataQualityExpectations"])
        self.assertEqual(bronze["writeMode"], "overwrite")

        self.assertEqual(silver["layer"], "silver")
        self.assertEqual(silver["sourceDetails"], {"table": "fip_poc.fip_poc_sc.customers_bronze"})
        self.assertEqual(silver["targetTable"], "fip_poc.fip_poc_sc.customers")
        self.assertEqual(silver["selectExp"][0], "CUSTOMER_ID AS customer_id")
        self.assertEqual(silver["whereClause"], ["STATE IS NOT NULL"])
        self.assertIsNone(silver["quarantineTable"])

    def test_rejects_non_cobol_source(self):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            json.dump([{"data_flow_id": "1", "data_flow_group": "A1", "source_format": "kafka"}], f)
            f.flush()
            with self.assertRaises(ValueError):
                build_specs(f.name, "dev")

    def test_onboard_overwrites_spec_table(self):
        spark = MagicMock()
        specs = onboard_dataflow_specs(spark, ONBOARDING, "dev", "fip_poc.fip_poc_sc.dataflowspec")
        spark.createDataFrame.assert_called_once_with(specs, SPEC_SCHEMA)
        writer = spark.createDataFrame.return_value.write
        writer.mode.assert_called_once_with("overwrite")
        writer.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            "fip_poc.fip_poc_sc.dataflowspec")


if __name__ == "__main__":
    unittest.main()
