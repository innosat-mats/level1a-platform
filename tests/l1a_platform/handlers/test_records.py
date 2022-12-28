
import numpy as np
from l1a_platform.handlers.records import (
    get_acs_fast_ops_records,
    get_acs_slow_ops_records,
    get_current_records,
    get_power_records,
    get_reconstructed_records,
    get_temperature_records,
    to_datetime,
    to_utc,
)


def test_get_power_records(h5_file):
    data = get_power_records(h5_file)
    expect_shape = (569,)
    assert data["time"].shape == expect_shape
    assert data["eciMpduMainBusVoltage"].shape == expect_shape
    assert data["ecoUnitPower_heatStr"].shape == expect_shape
    assert data["ecoUnitPower_plMain"].shape == expect_shape
    assert data["ecoUnitPower_plSafe"].shape == expect_shape


def test_get_current_records(h5_file):
    data = get_current_records(h5_file)
    expect_shape = (190,)
    assert data["time"].shape == expect_shape
    assert data["scoCurrentScMode"].shape == expect_shape


def test_get_temperature_records(h5_file):
    data = get_temperature_records(h5_file)
    expect_shape = (146, )
    assert data["time"].shape == expect_shape
    assert data["tcoTemp_pl"].shape == expect_shape
    assert data["tcoTemp_sa1_payloadIr2"].shape == expect_shape
    assert data["tcoTemp_sa2"].shape == expect_shape
    assert data["tcoTemp_str"].shape == expect_shape


def test_get_reconstructed_records(h5_file):
    data = get_reconstructed_records(h5_file)
    expect_rows = 5668
    assert data["time"].shape == (expect_rows,)
    assert np.array(data["afsAttitudeState"]).shape == (expect_rows, 4)
    assert np.array(data["afsGnssStateJ2000"]).shape == (expect_rows, 6)
    assert np.array(data["afsTPLongLatGeod"]).shape == (expect_rows, 2)
    assert np.array(data["afsTangentH_wgs84"]).shape == (expect_rows, 1)
    assert np.array(data["afsTangentPointECI"]).shape == (expect_rows, 3)


def test_get_acs_slow_ops_records(h5_file):
    data = get_acs_slow_ops_records(h5_file)
    expect_shape = (5681,)
    assert data["time"].shape == expect_shape
    assert data["aciTcLimbPointingAltOffset_0"].shape == expect_shape
    assert data["aciTcLimbPointingAltOffset_1"].shape == expect_shape
    assert data["aciTcLimbPointingAltOffset_2"].shape == expect_shape
    assert data["acoOnGnssPropagationTime"].shape == expect_shape
    assert data["acoOnGnssStateEcef_vx"].shape == expect_shape
    assert data["acoOnGnssStateEcef_vy"].shape == expect_shape
    assert data["acoOnGnssStateEcef_vz"].shape == expect_shape
    assert data["acoOnGnssStateEcef_x"].shape == expect_shape
    assert data["acoOnGnssStateEcef_y"].shape == expect_shape
    assert data["acoOnGnssStateEcef_z"].shape == expect_shape
    assert data["acoOnGnssStateJ2000_vx"].shape == expect_shape
    assert data["acoOnGnssStateJ2000_vy"].shape == expect_shape
    assert data["acoOnGnssStateJ2000_vz"].shape == expect_shape
    assert data["acoOnGnssStateJ2000_x"].shape == expect_shape
    assert data["acoOnGnssStateJ2000_y"].shape == expect_shape
    assert data["acoOnGnssStateJ2000_z"].shape == expect_shape
    assert data["acoOnGnssStateTime"].shape == expect_shape
    assert data["acoTleStateJ2000_vx"].shape == expect_shape
    assert data["acoTleStateJ2000_vy"].shape == expect_shape
    assert data["acoTleStateJ2000_vz"].shape == expect_shape
    assert data["acoTleStateJ2000_x"].shape == expect_shape
    assert data["acoTleStateJ2000_y"].shape == expect_shape
    assert data["acoTleStateJ2000_z"].shape == expect_shape
    assert data["acoTmArgGnssUsed"].shape == expect_shape
    assert data["acoTmArgInstrPitchAngle"].shape == expect_shape
    assert data["acoTmArgLpTp_x"].shape == expect_shape
    assert data["acoTmArgLpTp_y"].shape == expect_shape
    assert data["acoTmArgLpTp_z"].shape == expect_shape
    assert data["acoTmArgYawCompAngle"].shape == expect_shape


def test_get_acs_fast_ops_records(h5_file):
    data = get_acs_fast_ops_records(h5_file)
    expect_shape = (5682,)
    assert data["time"].shape == expect_shape
    assert data["afoArgFreezeEnabled"].shape == expect_shape
    assert data["afoTmAreAttitudeState_0"].shape == expect_shape
    assert data["afoTmAreAttitudeState_1"].shape == expect_shape
    assert data["afoTmAreAttitudeState_2"].shape == expect_shape
    assert data["afoTmAreAttitudeState_3"].shape == expect_shape
    assert data["afoTmAreRateScb_0"].shape == expect_shape
    assert data["afoTmAreRateScb_1"].shape == expect_shape
    assert data["afoTmAreRateScb_2"].shape == expect_shape


def test_to_utc():
    gps_datetimes = np.array([np.datetime64("2022-01-01T00:00:18")])
    utc_datetimes = np.array([np.datetime64("2022-01-01T00:00:00")])
    assert to_utc(gps_datetimes) == utc_datetimes


def test_to_datetime():
    scet = np.array([1352635371.1311798])
    assert to_datetime(scet) == np.datetime64("2022-11-16T12:02:33.131179776")
