# Derived from databrickslabs/sdp-meta (Databricks License, see LICENSE.txt).
# Modified: reduced to batch-only COBOL (Cobrix) ingestion; see NOTICE.
"""Wheel entry point: `sdp_meta onboard ...` and `sdp_meta run ...`."""
import argparse
import logging


def main():
    parser = argparse.ArgumentParser(prog="sdp_meta")
    commands = parser.add_subparsers(dest="command", required=True)
    onboard = commands.add_parser("onboard", help="load an onboarding file into the dataflowspec table")
    onboard.add_argument("--onboarding_file_path", required=True)
    onboard.add_argument("--env", required=True)
    onboard.add_argument("--dataflowspec_table", required=True)
    run = commands.add_parser("run", help="run the dataflows of a layer")
    run.add_argument("--layer", required=True, choices=["bronze", "silver"])
    run.add_argument("--dataflowspec_table", required=True)
    run.add_argument("--group")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"sdp-meta-{args.command}").getOrCreate()
    if args.command == "onboard":
        from sdp_meta.onboard_dataflowspec import onboard_dataflow_specs
        onboard_dataflow_specs(spark, args.onboarding_file_path, args.env, args.dataflowspec_table)
    else:
        from sdp_meta.dataflow_pipeline import DataflowPipeline
        DataflowPipeline.invoke_pipeline(spark, args.dataflowspec_table, args.layer, args.group)


if __name__ == "__main__":
    main()
