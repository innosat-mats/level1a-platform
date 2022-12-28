from enum import Enum
from typing import Dict

import h5py  # type: ignore
import numpy as np

TIME_KEY = "time"
GPS_START = np.datetime64("1980-01-06")
GPS_OFFSET = -np.timedelta64(18, 's')


class BaseRecord(Enum):

    @property
    def in_name(self) -> str:
        return self.value[0]

    @property
    def out_name(self) -> str:
        return self.value[1]


class PowerRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    main_bus_voltage = ("eciMpduMainBusVoltage", "eciMpduMainBusVoltage")
    power_heat_str = ("ecoUnitPower_heatStr", "ecoUnitPower_heatStr")
    power_pl_main = ("ecoUnitPower_plMain", "ecoUnitPower_plMain")
    power_pl_safe = ("ecoUnitPower_plSafe", "ecoUnitPower_plSafe")

    @property
    def path(self) -> str:
        return f"Level1A/HK_ecPowOps_1/{self.value[0]}"


class CurrentRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    sco_current_sc_mode = ("scoCurrentScMode", "scoCurrentScMode")

    @property
    def path(self) -> str:
        return f"Level1A/HK_scSysOps_1/{self.value[0]}"


class TemperatureRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    temp_pl = ("tcoTemp_pl", "tcoTemp_pl")
    temp_sa1 = ("tcoTemp_sa1_payloadIr2", "tcoTemp_sa1_payloadIr2")
    temp_sa2 = ("tcoTemp_sa2", "tcoTemp_sa2")
    temp_str = ("tcoTemp_str", "tcoTemp_str")

    @property
    def path(self) -> str:
        return f"Level1A/HK_tcThermEssential/{self.value[0]}"


class ReconstructedData(BaseRecord):
    time = ("Time", TIME_KEY)
    attitude_state = ("afsAttitudeState", "afsAttitudeState")
    gnss_state = ("afsGnssStateJ2000", "afsGnssStateJ2000")
    long_lat_geod = ("afsTPLongLatGeod", "afsTPLongLatGeod")
    tangent_wgs84 = ("afsTangentH_wgs84", "afsTangentH_wgs84")
    tangent_eci = ("afsTangentPointECI", "afsTangentPointECI")

    @property
    def path(self) -> str:
        return f"Level1A/ReconstructedData/{self.value[0]}"


class AcsSlowOpsRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    alt_offset_0 = ("aciTcLimbPointingAltOffset_0", "aciTcLimbPointingAltOffset_0")  # noqa: E501
    alt_offset_1 = ("aciTcLimbPointingAltOffset_1", "aciTcLimbPointingAltOffset_1")  # noqa: E501
    alt_offset_2 = ("aciTcLimbPointingAltOffset_2", "aciTcLimbPointingAltOffset_2")  # noqa: E501
    propagation_time = ("acoOnGnssPropagationTime", "acoOnGnssPropagationTime")
    ecef_vx = ("acoOnGnssStateEcef_vx", "acoOnGnssStateEcef_vx")
    ecef_vy = ("acoOnGnssStateEcef_vy", "acoOnGnssStateEcef_vy")
    ecef_vz = ("acoOnGnssStateEcef_vz", "acoOnGnssStateEcef_vz")
    ecef_x = ("acoOnGnssStateEcef_x", "acoOnGnssStateEcef_x")
    ecef_y = ("acoOnGnssStateEcef_y", "acoOnGnssStateEcef_y")
    ecef_z = ("acoOnGnssStateEcef_z", "acoOnGnssStateEcef_z")
    gnss_j2000_vx = ("acoOnGnssStateJ2000_vx", "acoOnGnssStateJ2000_vx")
    gnss_j2000_vy = ("acoOnGnssStateJ2000_vy", "acoOnGnssStateJ2000_vy")
    gnss_j2000_vz = ("acoOnGnssStateJ2000_vz", "acoOnGnssStateJ2000_vz")
    gnss_j2000_x = ("acoOnGnssStateJ2000_x", "acoOnGnssStateJ2000_x")
    gnss_j2000_y = ("acoOnGnssStateJ2000_y", "acoOnGnssStateJ2000_y")
    gnss_j2000_z = ("acoOnGnssStateJ2000_z", "acoOnGnssStateJ2000_z")
    gnss_state_time = ("acoOnGnssStateTime", "acoOnGnssStateTime")
    tle_j2000_vx = ("acoTleStateJ2000_vx", "acoTleStateJ2000_vx")
    tle_j2000_vy = ("acoTleStateJ2000_vy", "acoTleStateJ2000_vy")
    tle_j2000_vz = ("acoTleStateJ2000_vz", "acoTleStateJ2000_vz")
    tle_j2000_x = ("acoTleStateJ2000_x", "acoTleStateJ2000_x")
    tle_j2000_y = ("acoTleStateJ2000_y", "acoTleStateJ2000_y")
    tle_j2000_z = ("acoTleStateJ2000_z", "acoTleStateJ2000_z")
    tm_arg_gnss_used = ("acoTmArgGnssUsed", "acoTmArgGnssUsed")
    pitch_angle = ("acoTmArgInstrPitchAngle", "acoTmArgInstrPitchAngle")
    lptp_x = ("acoTmArgLpTp_x", "acoTmArgLpTp_x")
    lptp_y = ("acoTmArgLpTp_y", "acoTmArgLpTp_y")
    lptp_z = ("acoTmArgLpTp_z", "acoTmArgLpTp_z")
    yaw_comp_angle = ("acoTmArgYawCompAngle", "acoTmArgYawCompAngle")

    @property
    def path(self) -> str:
        return f"Level1A/TM_acAcsSlowOps/{self.value[0]}"


class AcsFastOpsRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    freeze_enabled = ("afoArgFreezeEnabled", "afoArgFreezeEnabled")
    attitude_state_0 = ("afoTmAreAttitudeState_0", "afoTmAreAttitudeState_0")
    attitude_state_1 = ("afoTmAreAttitudeState_1", "afoTmAreAttitudeState_1")
    attitude_state_2 = ("afoTmAreAttitudeState_2", "afoTmAreAttitudeState_2")
    attitude_state_3 = ("afoTmAreAttitudeState_3", "afoTmAreAttitudeState_3")
    rate_scb_0 = ("afoTmAreRateScb_0", "afoTmAreRateScb_0")
    rate_scb_1 = ("afoTmAreRateScb_1", "afoTmAreRateScb_1")
    rate_scb_2 = ("afoTmAreRateScb_2", "afoTmAreRateScb_2")

    @property
    def path(self) -> str:
        return f"Level1A/TM_afAcsFastOps/{self.value[0]}"


def to_utc(gps_datetimes: np.ndarray) -> np.ndarray:
    return gps_datetimes + GPS_OFFSET


def to_datetime(elapsed_seconds: np.ndarray) -> np.ndarray:
    return to_utc(GPS_START + (1e9*elapsed_seconds).astype("timedelta64[ns]"))


def get_power_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in PowerRecord}
    data[PowerRecord.time.out_name] = to_datetime(
        data[PowerRecord.time.out_name]
    )
    return data


def get_current_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in CurrentRecord}
    data[CurrentRecord.time.out_name] = to_datetime(
        data[CurrentRecord.time.out_name]
    )
    return data


def get_temperature_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in TemperatureRecord}
    data[TemperatureRecord.time.out_name] = to_datetime(
        data[TemperatureRecord.time.out_name]
    )
    return data


def get_reconstructed_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    return {
        ReconstructedData.time.out_name: to_datetime(
            np.array(h5_file[ReconstructedData.time.path]).squeeze()
        ),
        ReconstructedData.attitude_state.out_name: np.array(
            h5_file[ReconstructedData.attitude_state.path]
        ).T.tolist(),
        ReconstructedData.gnss_state.out_name: np.array(
            h5_file[ReconstructedData.gnss_state.path]
        ).T.tolist(),
        ReconstructedData.long_lat_geod.out_name: np.array(
            h5_file[ReconstructedData.long_lat_geod.path]
        ).T.tolist(),
        ReconstructedData.tangent_wgs84.out_name: np.array(
            h5_file[ReconstructedData.tangent_wgs84.path]
        ).T.tolist(),
        ReconstructedData.tangent_eci.out_name: np.array(
            h5_file[ReconstructedData.tangent_eci.path]
        ).T.tolist(),
    }


def get_acs_slow_ops_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in AcsSlowOpsRecord}
    data[AcsSlowOpsRecord.time.out_name] = to_datetime(
        data[AcsSlowOpsRecord.time.out_name]
    )
    return data


def get_acs_fast_ops_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in AcsFastOpsRecord}
    data[AcsFastOpsRecord.time.out_name] = to_datetime(
        data[AcsFastOpsRecord.time.out_name]
    )
    return data
