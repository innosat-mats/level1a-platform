import os
from tempfile import TemporaryDirectory
from unittest.mock import ANY, Mock, patch

import numpy as np
import pytest  # type: ignore
from l1a_platform.handlers.l1a_platform import (
    download_file,
    get_filename,
    get_or_raise,
    get_partitioned_dates,
    parse_event_message,
    read_to_table,
)
from l1a_platform.handlers.records import (
    get_acs_fast_ops_records,
    get_acs_slow_ops_records,
    get_current_records,
    get_power_records,
    get_reconstructed_records,
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
                "body": '{"bucket": "some-bucket", "object": "path/to/file.h5"}'  # noqa: E501
            }
        ]
    }
    assert parse_event_message(event) == ("path/to/file.h5", "some-bucket")


def test_get_partitioned_dates():
    datetimes = np.array([
        np.datetime64("2022-01-01T00:00:00"),
        np.datetime64("2022-01-02T00:00:00"),
    ])
    partitioned = get_partitioned_dates(datetimes)
    np.testing.assert_array_equal(partitioned["year"], [2022, 2022])
    np.testing.assert_array_equal(partitioned["month"], [1, 1])
    np.testing.assert_array_equal(partitioned["day"], [1, 2])


def test_download_file():
    mocked_client = Mock()
    bucket_name = "bucket"
    file_name = "file1"
    output_dir = TemporaryDirectory()
    download_file(mocked_client, bucket_name, file_name, output_dir)
    mocked_client.download_file.assert_called_once_with(
        'bucket',
        'file1',
        ANY,
    )


@pytest.mark.parametrize("getter", (
    get_power_records,
    get_current_records,
    get_temperature_records,
    get_reconstructed_records,
    get_acs_slow_ops_records,
    get_acs_fast_ops_records,
))
def test_read_to_table(h5_file, getter):
    assert read_to_table(getter, h5_file) is not None


def test_get_filename():
    h5_filename = "MATS_Platform_OPS_LEVEL1A_20221116-120226_20221116-120325.h5"
    parquet_filename = "MATS_Platform_OPS_LEVEL1A_20221116-120226_20221116-120325_{i}.parquet"  # noqa: E501
    assert get_filename(h5_filename) == parquet_filename
