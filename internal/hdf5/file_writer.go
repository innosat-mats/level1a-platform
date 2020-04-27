package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"gonum.org/v1/hdf5"
)

const (
	fname string = "LEVEL_1A_PLATF_20200120-094217_20200120-102136.hdf5"
)

// PowerRecord for variables in group: "HK_ecPowOps_1"
type PowerRecord struct {
	Time           float64
	MainBusVoltage float32 `json:"eciMpduMainBusVoltage"`
	PowerheatStr   float32 `json:"ecoUnitPower_heatStr"`
	PowerplMain    float32 `json:"ecoUnitPower_plMain"`
	PowerplSafe    float32 `json:"ecoUnitPower_plSafe"`
}

// CurrentRecord for variables in group: "HK_scSysOps_1"
type CurrentRecord struct {
	Time float64
	Mode uint8 `json:"scoCurrentScMode"`
}

// TemperatureRecord for variables in group: "HK_tcThermEssential"
type TemperatureRecord struct {
	Time            float64
	Temppl          float32 `json:"tcoTemp_pl"`
	Tempsa1         float32 `json:"tcoTemp_sa1"`
	Tempsa2         float32 `json:"tcoTemp_sa2"`
	Tempsc10pzPanel float32 `json:"tcoTemp_sc10_pzPanel"`
	Tempstr         float32 `json:"tcoTemp_str"`
}

// AttitudeRecord for variables in group: "PreciseAttitudeEstimation"
type AttitudeRecord struct {
	Time                float64
	AttitudeState       [4]float64    `json:"afsAttitudeState"`
	AttitudeUncertainty [3][3]float64 `json:"afsAttitudeUncertainty"`
	RateUncertainty     [3][3]float64 `json:"afsRateUncertainty"`
	SpacecraftRate      [3]float64    `json:"afsSpacecraftRate"`
	TangentPoint        [3]float64    `json:"afsTangentPoint"`
}

// OrbitRecord for variables in group: "PreciseOrbitEstimation"
type OrbitRecord struct {
	Time           float64
	GnssStateJ2000 [6]float64    `json:"acsGnssStateJ2000"`
	Uncertainty    [6][6]float64 `json:"acsNavigationUncertainty"`
}

// GnssRecord for variables in group: "TM_acGnssOps"
type GnssRecord struct {
	Time            float64
	PropagationTime uint8   `json:"acoOnGnssPropagationTime"`
	StateEcefVX     float32 `json:"acoOnGnssStateEcef_vx"`
	StateEcefVY     float32 `json:"acoOnGnssStateEcef_vy"`
	StateEcefVZ     float32 `json:"acoOnGnssStateEcef_vz"`
	StateEcefX      float32 `json:"acoOnGnssStateEcef_x"`
	StateEcefY      float32 `json:"acoOnGnssStateEcef_y"`
	StateEcefZ      float32 `json:"acoOnGnssStateEcef_z"`
	StateTime       float64 `json:"acoOnGnssStateTime"`
}

// Records collection of records
type Records struct {
	PowerRecords       []PowerRecord       `json:"HK_ecPowOps_1"`
	CurrentRecords     []CurrentRecord     `json:"HK_scSysOps_1"`
	TemperatureRecords []TemperatureRecord `json:"HK_tcThermEssential"`
	AttitudeRecords    []AttitudeRecord    `json:"PreciseAttitudeEstimation"`
	OrbitRecords       []OrbitRecord       `json:"PreciseOrbitEstimation"`
	GnssRecords        []GnssRecord        `json:"TM_acGnssOps"`
}

func getDatasetFloat64(
	filename string, groupname string, datasetname string) []float64 {

	f, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		panic(err)
	}

	group, err := f.OpenGroup(groupname)
	if err != nil {
		panic(err)
	}

	dataset, err := group.OpenDataset(datasetname)
	if err != nil {
		panic(err)
	}

	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]float64, npoints)
	err = dataset.Read(&data)
	if err != nil {
		panic(err)
	}
	dataset.Close()
	group.Close()
	f.Close()

	return data
}

func getDatasetFloat32(
	filename string, groupname string, datasetname string) []float32 {

	f, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		panic(err)
	}

	group, err := f.OpenGroup(groupname)
	if err != nil {
		panic(err)
	}

	dataset, err := group.OpenDataset(datasetname)
	if err != nil {
		panic(err)
	}

	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]float32, npoints)
	err = dataset.Read(&data)
	if err != nil {
		panic(err)
	}

	dataset.Close()
	group.Close()
	f.Close()

	return data
}

func getDatasetUint8(filename string, groupname string, datasetname string) []uint8 {

	f, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		panic(err)
	}

	group, err := f.OpenGroup(groupname)
	if err != nil {
		panic(err)
	}

	dataset, err := group.OpenDataset(datasetname)
	if err != nil {
		panic(err)
	}

	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]uint8, npoints)
	err = dataset.Read(&data)
	if err != nil {
		panic(err)
	}

	dataset.Close()
	group.Close()
	f.Close()

	return data
}

func getPowerRecords(filename string) []PowerRecord {

	var group string = "root/Level1A/HK_ecPowOps_1"

	time := getDatasetFloat64(filename, group, "Time")
	mainBusVoltage := getDatasetFloat32(filename, group, "eciMpduMainBusVoltage")
	powerheatStr := getDatasetFloat32(filename, group, "ecoUnitPower_heatStr")
	powerplMain := getDatasetFloat32(filename, group, "ecoUnitPower_plMain")
	powerplSafe := getDatasetFloat32(filename, group, "ecoUnitPower_plSafe")

	var records []PowerRecord
	for i := 0; i < len(time); i++ {
		record := PowerRecord{
			Time:           time[i],
			MainBusVoltage: mainBusVoltage[i],
			PowerheatStr:   powerheatStr[i],
			PowerplMain:    powerplMain[i],
			PowerplSafe:    powerplSafe[i],
		}
		records = append(records, record)
	}
	return records

}

func getCurrentRecords(filename string) []CurrentRecord {

	var group string = "root/Level1A/HK_scSysOps_1"

	time := getDatasetFloat64(filename, group, "Time")
	mode := getDatasetUint8(filename, group, "scoCurrentScMode")

	var records []CurrentRecord
	for i := 0; i < len(time); i++ {
		record := CurrentRecord{
			Time: time[i],
			Mode: mode[i],
		}
		records = append(records, record)
	}
	return records
}

func getTemperatureRecords(filename string) []TemperatureRecord {

	var group string = "root/Level1A/HK_tcThermEssential"

	time := getDatasetFloat64(filename, group, "Time")
	temppl := getDatasetFloat32(filename, group, "tcoTemp_pl")
	tempsa1 := getDatasetFloat32(filename, group, "tcoTemp_sa1")
	tempsa2 := getDatasetFloat32(filename, group, "tcoTemp_sa2")
	tempsc10pzPanel := getDatasetFloat32(filename, group, "tcoTemp_sc10_pzPanel")
	tempstr := getDatasetFloat32(filename, group, "tcoTemp_str")

	var records []TemperatureRecord
	for i := 0; i < len(time); i++ {
		record := TemperatureRecord{
			Time:            time[i],
			Temppl:          temppl[i],
			Tempsa1:         tempsa1[i],
			Tempsa2:         tempsa2[i],
			Tempsc10pzPanel: tempsc10pzPanel[i],
			Tempstr:         tempstr[i],
		}
		records = append(records, record)
	}
	return records
}

func getGnssRecords(filename string) []GnssRecord {

	var group string = "root/Level1A/TM_acGnssOps"

	time := getDatasetFloat64(filename, group, "Time")
	propagationTime := getDatasetUint8(filename, group, "acoOnGnssPropagationTime")
	stateEcefVX := getDatasetFloat32(filename, group, "acoOnGnssStateEcef_vx")
	stateEcefVY := getDatasetFloat32(filename, group, "acoOnGnssStateEcef_vy")
	stateEcefVZ := getDatasetFloat32(filename, group, "acoOnGnssStateEcef_vz")
	stateEcefX := getDatasetFloat32(filename, group, "acoOnGnssStateEcef_x")
	stateEcefY := getDatasetFloat32(filename, group, "acoOnGnssStateEcef_y")
	stateEcefZ := getDatasetFloat32(filename, group, "acoOnGnssStateEcef_z")
	stateTime := getDatasetFloat64(filename, group, "acoOnGnssStateTime")

	var records []GnssRecord
	for i := 0; i < len(time); i++ {
		record := GnssRecord{
			Time:            time[i],
			PropagationTime: propagationTime[i],
			StateEcefVX:     stateEcefVX[i],
			StateEcefVY:     stateEcefVY[i],
			StateEcefVZ:     stateEcefVZ[i],
			StateEcefX:      stateEcefX[i],
			StateEcefY:      stateEcefY[i],
			StateEcefZ:      stateEcefZ[i],
			StateTime:       stateTime[i],
		}
		records = append(records, record)
	}
	return records

}

func getOrbitRecords(filename string) []OrbitRecord {

	var group string = "root/Level1A/ReconstructedData/PreciseOrbitEstimation"

	time := getDatasetFloat64(filename, group, "Time")
	gnssStateJ2000 := getDatasetFloat64(filename, group, "acsGnssStateJ2000")
	uncertainty := getDatasetFloat64(filename, group, "acsNavigationUncertainty")

	var records []OrbitRecord

	//uncertainty is common to all records
	var uncertainty2d [6][6]float64
	for i := 0; i < 6; i++ {
		for j := 0; j < 6; j++ {
			uncertainty2d[i][j] = uncertainty[i*6+j]
		}
	}
	n := len(time)
	for i := 0; i < n; i++ {
		record := OrbitRecord{
			Time:        time[i],
			Uncertainty: uncertainty2d,
		}
		for j := 0; j < 6; j++ {
			record.GnssStateJ2000[j] = gnssStateJ2000[i+j*n]
		}
		records = append(records, record)
	}
	return records
}

func getAttitudeRecords(filename string) []AttitudeRecord {

	var group string = "root/Level1A/ReconstructedData/PreciseAttitudeEstimation"

	time := getDatasetFloat64(filename, group, "Time")
	attitudeState := getDatasetFloat64(filename, group, "afsAttitudeState")
	attitudeUncertainty := getDatasetFloat64(filename, group, "afsAttitudeUncertainty")
	rateUncertainty := getDatasetFloat64(filename, group, "afsRateUncertainty")
	spacecraftRate := getDatasetFloat64(filename, group, "afsSpacecraftRate")
	tangentPoint := getDatasetFloat64(filename, group, "afsTangentPoint")

	var records []AttitudeRecord
	n := len(time)
	for i := 0; i < n; i++ {
		record := AttitudeRecord{Time: time[i]}
		for j := 0; j < 3; j++ {
			record.SpacecraftRate[j] = spacecraftRate[i+j*n]
			record.TangentPoint[j] = tangentPoint[i+j*n]
		}
		for j := 0; j < 4; j++ {
			record.AttitudeState[j] = attitudeState[i+j*n]
		}
		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				record.AttitudeUncertainty[j][k] = attitudeUncertainty[i*9+j*3+k]
				record.RateUncertainty[j][k] = rateUncertainty[i*9+j*3+k]
			}
		}
		records = append(records, record)
	}
	return records
}

func toDateTime(secondsSinceEpoch float64) string {
	dt := time.Duration(secondsSinceEpoch) * time.Second
	start := time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC)
	datetime := start.Add(dt)
	out := datetime.Format(time.RFC3339)
	return out
}

func main() {

	powerRecords := getPowerRecords(fname)

	currentRecords := getCurrentRecords(fname)

	temperatureRecords := getTemperatureRecords(fname)

	gnssRecords := getGnssRecords(fname)

	orbitRecords := getOrbitRecords(fname)

	attitudeRecords := getAttitudeRecords(fname)

	records := Records{
		PowerRecords:       powerRecords,
		CurrentRecords:     currentRecords,
		TemperatureRecords: temperatureRecords,
		AttitudeRecords:    attitudeRecords,
		OrbitRecords:       orbitRecords,
		GnssRecords:        gnssRecords,
	}
	outdata, _ := json.MarshalIndent(records, "", "    ")
	_ = ioutil.WriteFile("test.json", outdata, 0644)

	//t0 := toDateTime(a1[0].time)
	fmt.Println(outdata)

}
