
import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3
import h5py  # type: ignore
import numpy as np
import pyarrow as pa  # type: ignore
import pyarrow.dataset as ds  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from .records import (
    TIME_KEY,
    get_attitude_records,
    get_current_records,
    get_gnss_records,
    get_hirate_attitude_records,
    get_orbit_records,
    get_power_records,
    get_temperature_records,
)

S3Client = Any
Event = Dict[str, Any]
Context = Any
Getter = Callable[[h5py.File], Dict[str, np.ndarray]]

file_suffix: Dict[Callable, str] = {
    get_power_records: "HK_ecPowOps_1",
    get_current_records: "scoCurrentScMode",
    get_temperature_records: "HK_tcThermEssential",
    get_attitude_records: "PreciseAttitudeEstimation",
    get_orbit_records: "PreciseOrbitEstimation",
    get_gnss_records: "TM_acGnssOps",
    get_hirate_attitude_records: "TM_afAcsHiRateAttitudeData",
}


def get_or_raise(variable_name: str) -> str:
    if (var := os.environ.get(variable_name)) is None:
        raise EnvironmentError(
            f"{variable_name} is a required environment variable"
        )
    return var


def parse_event_message(event: Event) -> Tuple[List[str], str]:
    message: Dict[str, Any] = json.loads(event["Records"][0]["body"])
    bucket = message["bucket"]
    objects = message["objects"]
    return objects, bucket


def get_partitioned_dates(datetimes: np.ndarray) -> Dict[str, np.ndarray]:
    Y, M, D = [datetimes.astype(f"M8[{x}]") for x in "YMD"]
    return {
        "year": Y.astype(int) + 1970,
        "month": (M - Y + 1).astype(int),
        "day": (D - M + 1).astype(int),
    }


def download_files(
    s3_client: S3Client,
    bucket_name: str,
    file_names: List[str],
    output_dir: TemporaryDirectory,
) -> List[Path]:
    out_files = []
    for file_name in file_names:
        file_path = Path(f"{output_dir.name}/{file_name}")
        with open(file_path, "wb") as h5_file:
            s3_client.download_fileobj(
                bucket_name,
                file_name,
                h5_file,
            )
            out_files.append(file_path)
    return out_files


def read_to_table(getter: Getter, h5_file: h5py.File) -> Optional[pa.Table]:
    data = getter(h5_file)

    data.update(get_partitioned_dates(data[TIME_KEY]))
    try:
        return pa.Table.from_pydict(data)
    except pa.ArrowException:
        msg = f"Failed to create table with {getter} from file {h5_file.filename}"  # noqa: E501
        logging.exception(msg)
    return None


def get_filename(files: List[Path], getter: Getter) -> str:
    return f"{hash(tuple(files))}_{file_suffix[getter]}" + "_{i}.parquet"


def lambda_handler(event: Event, context: Context):
    out_bucket = get_or_raise("OUTPUT_BUCKET")
    region = os.environ.get('AWS_REGION', "eu-north-1")
    objects, in_bucket = parse_event_message(event)
    tempdir = TemporaryDirectory()

    s3_client = boto3.client('s3')
    files = download_files(s3_client, in_bucket, objects, tempdir)
    h5_files = [h5py.File(h5_file_path) for h5_file_path in files]

    for f in (
        get_power_records,
        get_current_records,
        get_temperature_records,
        get_attitude_records,
        get_orbit_records,
        get_gnss_records,
        get_hirate_attitude_records,
    ):
        tables: List[h5py.Table] = list(filter(None, [
            read_to_table(f, h5) for h5 in h5_files
        ]))
        if len(tables) > 0:
            pq.write_to_dataset(
                table=tables,
                root_path=out_bucket,
                basename_template=get_filename(files, f),
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
