from pathlib import Path

import h5py  # type: ignore
import pytest  # type: ignore


@pytest.fixture
def h5_file() -> h5py.File:
    h5_path = Path(__file__).parent / "files" / "MATS_Platform_OPS_LEVEL1A_20221116-120226_20221116-120325.h5"  # noqa: E501
    return h5py.File(h5_path)
