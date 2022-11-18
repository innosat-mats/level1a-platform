#!/usr/bin/env python3

from aws_cdk import App

from stacks.l1a_platform_stack import L1APlatformStack

app = App()

L1APlatformStack(
    app,
    "L1APlatformToParquetStack",
    input_bucket_name="l1-platform-h5",
    output_bucket_name="mats-l1a-platform-parquet",
    queue_arn_export_name="L0PlatformFetcherStackOutputQueue",
)

app.synth()
