import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import ANY, Mock, call, patch

import numpy as np
import pytest  # type: ignore
from l1a_platform.handlers.l1a_platform import (
    download_files,
    get_filename,
    get_or_raise,
    get_partitioned_dates,
    parse_event_message,
    read_to_table,
)
from l1a_platform.handlers.records import (
    get_attitude_records,
    get_current_records,
    get_gnss_records,
    get_hirate_attitude_records,
    get_orbit_records,
    get_power_records,
    get_temperature_records,
)


@patch.dict(os.environ, {"DEFINITELY": "set"})
def test_get_or_raise():
    assert get_or_raise("DEFINITELY") == "set"


def test_get_or_raise_raises():
    with pytest.raises(
        EnvironmentError,
        match="DEFINITELYNOT is a required environment variable"
    ):
        get_or_raise("DEFINITELYNOT")


def test_parse_event_message():
    event = {
        "Records": [
            {
                "body": '{"bucket": "some-bucket", "objects": ["path/to/file.h5"]}'  # noqa: E501
            }
        ]
    }
    assert parse_event_message(event) == (["path/to/file.h5"], "some-bucket")


def test_get_partitioned_dates():
    datetimes = np.array([
        np.datetime64("2022-01-01T00:00:00"),
        np.datetime64("2022-01-02T00:00:00"),
    ])
    partitioned = get_partitioned_dates(datetimes)
    np.testing.assert_array_equal(partitioned["year"], [2022, 2022])
    np.testing.assert_array_equal(partitioned["month"], [1, 1])
    np.testing.assert_array_equal(partitioned["day"], [1, 2])


def test_download_files():
    mocked_client = Mock()
    bucket_name = "bucket"
    file_names = ["file1", "file2"]
    output_dir = TemporaryDirectory()
    download_files(mocked_client, bucket_name, file_names, output_dir)
    mocked_client.download_fileobj.assert_has_calls([
        call('bucket', 'file1', ANY),
        call('bucket', 'file2', ANY),
    ])


@pytest.mark.parametrize("getter", (
    get_power_records,
    get_current_records,
    get_temperature_records,
    get_attitude_records,
    get_orbit_records,
    get_gnss_records,
    get_hirate_attitude_records,
))
def test_read_to_table(h5_file, getter):
    assert read_to_table(getter, h5_file) is not None


@pytest.mark.parametrize("getter,extension", (
    (get_power_records, "HK_ecPowOps_1"),
    (get_current_records, "scoCurrentScMode"),
    (get_temperature_records, "HK_tcThermEssential"),
    (get_attitude_records, "PreciseAttitudeEstimation"),
    (get_orbit_records, "PreciseOrbitEstimation"),
    (get_gnss_records, "TM_acGnssOps"),
    (get_hirate_attitude_records, "TM_afAcsHiRateAttitudeData"),
))
def test_get_filename(getter, extension):
    files = [Path("example.h5"), Path("anotherone.h5")]

    fname = get_filename(files, getter)
    match = re.search(r'([0-9]+_)(.*)(_{i}.parquet)', fname)
    assert match is not None
    assert match[2] == extension
    assert match[3] == "_{i}.parquet"
