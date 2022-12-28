
import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Optional, Tuple

import boto3
import h5py  # type: ignore
import numpy as np
import pyarrow as pa  # type: ignore
import pyarrow.dataset as ds  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from .records import (
    TIME_KEY,
    get_acs_fast_ops_records,
    get_acs_slow_ops_records,
    get_current_records,
    get_power_records,
    get_reconstructed_records,
    get_temperature_records,
)

S3Client = Any
Event = Dict[str, Any]
Context = Any
Getter = Callable[[h5py.File], Dict[str, np.ndarray]]

folder_prefix: Dict[Callable, str] = {
    get_power_records: "HK_ecPowOps_1",
    get_current_records: "scoCurrentScMode",
    get_temperature_records: "HK_tcThermEssential",
    get_reconstructed_records: "ReconstructedData",
    get_acs_slow_ops_records: "TM_acAcsSlowOps",
    get_acs_fast_ops_records: "TM_afAcsFastOps",
}


class NothingToDo(Exception):
    pass


def get_or_raise(variable_name: str) -> str:
    if (var := os.environ.get(variable_name)) is None:
        raise EnvironmentError(
            f"{variable_name} is a required environment variable"
        )
    return var


def parse_event_message(event: Event) -> Tuple[str, str]:
    message: Dict[str, Any] = json.loads(event["Records"][0]["body"])
    object = message["object"]
    bucket = message["bucket"]
    return object, bucket


def get_partitioned_dates(datetimes: np.ndarray) -> Dict[str, np.ndarray]:
    Y, M, D = [datetimes.astype(f"M8[{x}]") for x in "YMD"]
    return {
        "year": Y.astype(int) + 1970,
        "month": (M - Y + 1).astype(int),
        "day": (D - M + 1).astype(int),
    }


def download_file(
    s3_client: S3Client,
    bucket_name: str,
    file_name: str,
    output_dir: TemporaryDirectory,
) -> Path:
    file_path = Path(f"{output_dir.name}/{file_name}")
    s3_client.download_file(bucket_name, file_name, str(file_path))
    return file_path


def read_to_table(getter: Getter, h5_file: h5py.File) -> Optional[pa.Table]:
    data = getter(h5_file)

    data.update(get_partitioned_dates(data[TIME_KEY]))
    try:
        return pa.Table.from_pydict(data)
    except pa.ArrowException:
        msg = f"Failed to create table with {getter} from file {h5_file.filename}"  # noqa: E501
        logging.exception(msg)
    return None


def get_filename(h5_filename: str) -> str:
    return h5_filename.removesuffix(".h5") + "_{i}.parquet"


def lambda_handler(event: Event, context: Context):
    out_bucket = get_or_raise("OUTPUT_BUCKET")
    region = os.environ.get('AWS_REGION', "eu-north-1")
    object, in_bucket = parse_event_message(event)
    tempdir = TemporaryDirectory()

    s3_client = boto3.client('s3')
    file = download_file(s3_client, in_bucket, object, tempdir)
    h5_file = h5py.File(file)

    for f in (
        get_power_records,
        get_current_records,
        get_temperature_records,
        get_reconstructed_records,
        get_acs_slow_ops_records,
        get_acs_fast_ops_records,
    ):
        pq.write_to_dataset(
            table=read_to_table(f, h5_file),
            root_path=f"{out_bucket}/{folder_prefix[f]}",
            basename_template=get_filename(object),
            existing_data_behavior="overwrite_or_ignore",
            filesystem=pa.fs.S3FileSystem(
                region=region,
                request_timeout=10,
                connect_timeout=10
            ),
            partitioning=ds.partitioning(
                schema=pa.schema([
                    ('year', pa.int32()),
                    ('month', pa.int32()),
                    ('day', pa.int32()),
                ]),
            ),
            version='2.6',
        )
