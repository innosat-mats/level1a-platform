
import numpy as np
from l1a_platform.handlers.records import (
    get_attitude_records,
    get_current_records,
    get_gnss_records,
    get_hirate_attitude_records,
    get_orbit_records,
    get_power_records,
    get_temperature_records,
    to_datetime,
    to_utc,
)


def test_get_power_records(h5_file):
    data = get_power_records(h5_file)
    assert data["time"].shape == (6,)
    assert data["eciMpduMainBusVoltage"].shape == (6,)
    assert data["ecoUnitPower_heatStr"].shape == (6,)
    assert data["ecoUnitPower_plMain"].shape == (6,)
    assert data["ecoUnitPower_plSafe"].shape == (6,)


def test_get_current_records(h5_file):
    data = get_current_records(h5_file)
    assert data["time"].shape == (2,)
    assert data["scoCurrentScMode"].shape == (2,)


def test_get_temperature_records(h5_file):
    data = get_temperature_records(h5_file)
    assert data["time"].shape == (1,)
    assert data["tcoTemp_pl"].shape == (1,)
    assert data["tcoTemp_sa1_payloadIr2"].shape == (1,)
    assert data["tcoTemp_sa2"].shape == (1,)
    assert data["tcoTemp_str"].shape == (1,)


def test_get_attitude_records(h5_file):
    data = get_attitude_records(h5_file)
    assert data["time"].shape == (63,)
    assert np.array(data["afsAttitudeState"]).shape == (63, 4)
    assert np.array(data["afsAttitudeUncertainty"]).shape == (63, 3, 3)
    assert np.array(data["afsRateUncertainty"]).shape == (63, 3, 3)
    assert np.array(data["afsSpacecraftRate"]).shape == (63, 3)


def test_get_orbit_records(h5_file):
    data = get_orbit_records(h5_file)
    assert data["time"].shape == (63,)
    assert np.array(data["acsGnssStateJ2000"]).shape == (63, 6)
    assert np.array(data["acsNavigationUncertainty"]).shape == (63, 6, 6)
    assert np.array(data["afsTangentPoint"]).shape == (63, 3)


def test_get_gnss_records(h5_file):
    data = get_gnss_records(h5_file)
    assert data["time"].shape == (63,)
    assert data["acoOnGnssPropagationTime"].shape == (63,)
    assert data["acoOnGnssStateEcef_vx"].shape == (63,)
    assert data["acoOnGnssStateEcef_vy"].shape == (63,)
    assert data["acoOnGnssStateEcef_vz"].shape == (63,)
    assert data["acoOnGnssStateEcef_x"].shape == (63,)
    assert data["acoOnGnssStateEcef_y"].shape == (63,)
    assert data["acoOnGnssStateEcef_z"].shape == (63,)
    assert data["acoOnGnssStateTime"].shape == (63,)
    assert data["acoOnGnssValid"].shape == (63,)


def test_get_hirate_attitude_records(h5_file):
    data = get_hirate_attitude_records(h5_file)
    assert data["time"].shape == (157,)
    assert data["afiStrMasterReturn"].shape == (157,)
    assert data["afiStrResQuaternion_q0"].shape == (157,)
    assert data["afiStrResQuaternion_q1"].shape == (157,)
    assert data["afiStrResQuaternion_q2"].shape == (157,)
    assert data["afiStrResQuaternion_q3"].shape == (157,)
    assert data["afoRsaCumSumAngleScb_x"].shape == (157,)
    assert data["afoRsaCumSumAngleScb_y"].shape == (157,)
    assert data["afoRsaCumSumAngleScb_z"].shape == (157,)
    assert data["afoRsaValidityCount"].shape == (157,)
    assert data["afoTmMhObt"].shape == (157,)


def test_to_utc():
    gps_datetimes = np.array([np.datetime64("2022-01-01T00:00:18")])
    utc_datetimes = np.array([np.datetime64("2022-01-01T00:00:00")])
    assert to_utc(gps_datetimes) == utc_datetimes


def test_to_datetime():
    scet = np.array([1352635371.1311798])
    assert to_datetime(scet) == np.datetime64("2022-11-16T12:02:33.131179776")
