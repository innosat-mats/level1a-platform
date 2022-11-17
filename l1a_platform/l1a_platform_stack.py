
from aws_cdk import Duration, Fn, Stack
from aws_cdk.aws_lambda import Architecture, Runtime
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_lambda_python_alpha import PythonFunction  # type: ignore
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_sqs import Queue
from constructs import Construct


class L1APlatformStack(Stack):

    def __init__(
        self,
        scope: Construct,
        id: str,
        input_bucket_name: str,
        output_bucket_name: str,
        queue_arn_export_name: str,
        lambda_timeout: Duration = Duration.seconds(300),
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        input_bucket = Bucket.from_bucket_name(
            self,
            "L1APlatformInputBucket",
            input_bucket_name,
        )

        output_bucket = Bucket.from_bucket_name(
            self,
            "L1APlatformOutputBucket",
            output_bucket_name,
        )

        event_queue = Queue.from_queue_arn(
            self,
            "L1APlatformQueue",
            Fn.import_value(queue_arn_export_name)
        )

        platform_lambda = PythonFunction(
            self,
            "L1APlatformLambda",
            entry="./l1a_platform/handlers",
            handler="lambda_handler",
            index="l1a_platform.py",
            timeout=lambda_timeout,
            architecture=Architecture.X86_64,
            runtime=Runtime.PYTHON_3_9,
            memory_size=512,
            environment={
                "OUTPUT_BUCKET": output_bucket_name,
            },
            events=[SqsEventSource(event_queue, batch_size=1)],
        )

        output_bucket.grant_read_write(platform_lambda)
        input_bucket.grant_read(platform_lambda)
