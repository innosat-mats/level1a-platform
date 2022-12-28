from pathlib import Path

import h5py  # type: ignore
import pytest  # type: ignore


@pytest.fixture
def h5_file() -> h5py.File:
    h5_path = Path(__file__).parent / "files" / "MATS_Platform_OPS_LEVEL1A_20221221-132239_20221221-132351.h5"  # noqa: E501
    return h5py.File(h5_path)
