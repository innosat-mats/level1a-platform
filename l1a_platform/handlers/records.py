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
    time = ("__common__/SCET", TIME_KEY)
    main_bus_voltage = ("eciMpduMainBusVoltage/raw", "eciMpduMainBusVoltage")
    power_heat_str = ("ecoUnitPower_heatStr/raw", "ecoUnitPower_heatStr")
    power_pl_main = ("ecoUnitPower_plMain/raw", "ecoUnitPower_plMain")
    power_pl_safe = ("ecoUnitPower_plSafe/raw", "ecoUnitPower_plSafe")

    @property
    def path(self) -> str:
        return f"Level1A/HK_ecPowOps_1/{self.value[0]}"


class CurrentRecord(BaseRecord):
    time = ("SCET", TIME_KEY)
    sco_current_sc_mode = ("raw", "scoCurrentScMode")

    @property
    def path(self) -> str:
        return f"Level1A/scoCurrentScMode/{self.value[0]}"


class TemperatureRecord(BaseRecord):
    time = ("__common__/SCET", TIME_KEY)
    temp_pl = ("tcoTemp_pl/raw", "tcoTemp_pl")
    temp_sa1 = ("tcoTemp_sa1_payloadIr2/raw", "tcoTemp_sa1_payloadIr2")
    temp_sa2 = ("tcoTemp_sa2/raw", "tcoTemp_sa2")
    temp_str = ("tcoTemp_str/raw", "tcoTemp_str")

    @property
    def path(self) -> str:
        return f"Level1A/HK_tcThermEssential/{self.value[0]}"


class AttitudeRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    attitude_state = ("afsAttitudeState", "afsAttitudeState")
    attitude_uncertainty = ("afsAttitudeUncertainty", "afsAttitudeUncertainty")
    rate_uncertainty = ("afsRateUncertainty", "afsRateUncertainty")
    spacecraft_rate = ("afsSpacecraftRate", "afsSpacecraftRate")
    tangent_point = ("afsTangentPoint", "afsTangentPoint")

    @property
    def path(self) -> str:
        return f"Level1A/ReconstructedData/PreciseAttitudeEstimation/{self.value[0]}"  # noqa: E501


class OrbitRecord(BaseRecord):
    time = ("Time", TIME_KEY)
    gnss_state = ("acsGnssStateJ2000", "acsGnssStateJ2000")
    navigation_uncertainty = (
        "acsNavigationUncertainty",
        "acsNavigationUncertainty"
    )

    @property
    def path(self) -> str:
        return f"Level1A/ReconstructedData/PreciseOrbitEstimation/{self.value[0]}"  # noqa: E501


class GnssRecord(BaseRecord):
    time = ("__common__/SCET", TIME_KEY)
    propagation_time = (
        "acoOnGnssPropagationTime/raw",
        "acoOnGnssPropagationTime"
    )
    state_vx = ("acoOnGnssStateEcef_vx/raw", "acoOnGnssStateEcef_vx")
    state_vy = ("acoOnGnssStateEcef_vy/raw", "acoOnGnssStateEcef_vy")
    state_vz = ("acoOnGnssStateEcef_vz/raw", "acoOnGnssStateEcef_vz")
    state_x = ("acoOnGnssStateEcef_x/raw", "acoOnGnssStateEcef_x")
    state_y = ("acoOnGnssStateEcef_y/raw", "acoOnGnssStateEcef_y")
    state_z = ("acoOnGnssStateEcef_z/raw", "acoOnGnssStateEcef_z")
    state_time = ("acoOnGnssStateTime/raw", "acoOnGnssStateTime")
    valid = ("acoOnGnssValid/raw", "acoOnGnssValid")

    @property
    def path(self) -> str:
        return f"Level1A/TM_acGnssOps/{self.value[0]}"


class HiRateAttitudeRecord(BaseRecord):
    time = ("__common__/SCET", TIME_KEY)
    master_return = ("afiStrMasterReturn/raw", "afiStrMasterReturn")
    res_quaternion_q0 = ("afiStrResQuaternion_q0/raw", "afiStrResQuaternion_q0")
    res_quaternion_q1 = ("afiStrResQuaternion_q1/raw", "afiStrResQuaternion_q1")
    res_quaternion_q2 = ("afiStrResQuaternion_q2/raw", "afiStrResQuaternion_q2")
    res_quaternion_q3 = ("afiStrResQuaternion_q3/raw", "afiStrResQuaternion_q3")
    cum_sum_angle_x = ("afoRsaCumSumAngleScb_x/raw", "afoRsaCumSumAngleScb_x")
    cum_sum_angle_y = ("afoRsaCumSumAngleScb_y/raw", "afoRsaCumSumAngleScb_y")
    cum_sum_angle_z = ("afoRsaCumSumAngleScb_z/raw", "afoRsaCumSumAngleScb_z")
    validity_count = ("afoRsaValidityCount/raw", "afoRsaValidityCount")
    mh_obt = ("afoTmMhObt/raw", "afoTmMhObt")

    @property
    def path(self) -> str:
        return f"Level1A/TM_afAcsHiRateAttitudeData/{self.value[0]}"


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


def get_attitude_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    return {
        AttitudeRecord.time.out_name: to_datetime(
            np.array(h5_file[AttitudeRecord.time.path]).squeeze()
        ),
        AttitudeRecord.attitude_state.out_name: np.array(
            h5_file[AttitudeRecord.attitude_state.path]
        ).T.tolist(),
        AttitudeRecord.attitude_uncertainty.out_name: np.array(
            h5_file[AttitudeRecord.attitude_uncertainty.path]
        ).tolist(),
        AttitudeRecord.rate_uncertainty.out_name: np.array(
            h5_file[AttitudeRecord.rate_uncertainty.path]
        ).tolist(),
        AttitudeRecord.spacecraft_rate.out_name: np.array(
            h5_file[AttitudeRecord.spacecraft_rate.path]
        ).T.tolist(),
    }


def get_orbit_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    time = to_datetime(np.array(h5_file[OrbitRecord.time.path]).squeeze())
    gnss = np.array(h5_file[OrbitRecord.gnss_state.path]).T
    uncertainty = np.array(h5_file[OrbitRecord.navigation_uncertainty.path])
    return {
        OrbitRecord.time.out_name: time,
        OrbitRecord.gnss_state.out_name: gnss.tolist(),
        OrbitRecord.navigation_uncertainty.out_name: (
            uncertainty[np.newaxis, :, :].repeat(len(time), axis=0).tolist()
        ),
        #  AttitudeRecord.tangent_point is added here due to having its time
        #  axis shared with orbit records rather than attitude records.
        AttitudeRecord.tangent_point.out_name: np.array(
            h5_file[AttitudeRecord.tangent_point.path]
        ).T.tolist(),
    }


def get_gnss_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in GnssRecord}
    data[GnssRecord.time.out_name] = to_datetime(data[GnssRecord.time.out_name])
    return data


def get_hirate_attitude_records(h5_file: h5py.File) -> Dict[str, np.ndarray]:
    data = {e.out_name: np.array(h5_file[e.path]) for e in HiRateAttitudeRecord}
    data[HiRateAttitudeRecord.time.out_name] = to_datetime(
        data[HiRateAttitudeRecord.time.out_name]
    )
    return data
