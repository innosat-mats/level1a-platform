package filewriter

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	"gonum.org/v1/hdf5"
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

func getDataset(
	filename string, groupname string, datasetname string) *hdf5.Dataset {

	f, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalln(err, filename)
	}
	group, err := f.OpenGroup(groupname)
	if err != nil {
		log.Fatalln(err, filename, groupname)
	}

	dataset, err := group.OpenDataset(datasetname)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname)
	}
	return dataset
}

func getDatasetFloat64(
	filename string, groupname string, datasetname string) []float64 {

	dataset := getDataset(filename, groupname, datasetname)
	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]float64, npoints)
	err := dataset.Read(&data)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname, "data")
	}
	dataset.Close()
	return data
}

func getDatasetFloat32(
	filename string, groupname string, datasetname string) []float32 {

	dataset := getDataset(filename, groupname, datasetname)
	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]float32, npoints)
	err := dataset.Read(&data)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname, "data")
	}
	dataset.Close()
	return data
}

func getDatasetUint8(
	filename string, groupname string, datasetname string) []uint8 {

	dataset := getDataset(filename, groupname, datasetname)
	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]uint8, npoints)
	err := dataset.Read(&data)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname, "data")
	}
	dataset.Close()
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
	for i := range time {
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
	for i := range time {
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
	for i := range time {
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
	for i := range time {
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

	//uncertainty is common to all records
	uncertainty2d := to6by6arr(uncertainty)

	var records []OrbitRecord
	n := len(time)
	for i := range time {
		record := OrbitRecord{
			Time:        time[i],
			Uncertainty: uncertainty2d,
		}
		copy(record.GnssStateJ2000[:], to1DSlice(gnssStateJ2000, 6, n, i))
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
	for i := range time {
		record := AttitudeRecord{Time: time[i]}
		copy(record.SpacecraftRate[:], to1DSlice(spacecraftRate, 3, n, i))
		copy(record.TangentPoint[:], to1DSlice(tangentPoint, 3, n, i))
		copy(record.AttitudeState[:], to1DSlice(attitudeState, 4, n, i))
		record.AttitudeUncertainty = to3by3arr(attitudeUncertainty, i)
		record.RateUncertainty = to3by3arr(rateUncertainty, i)
		records = append(records, record)
	}
	return records
}

func to1DSlice(inslice []float64, sizeDim1 int, sizeDim2, offset int) []float64 {
	//returns a 1-d slice extracted from a 1-d slice that is a representation
	//of a 2-Darray with shape (sizeDim1, sizeDim2)
	outslice := make([]float64, sizeDim1)
	for i := 0; i < sizeDim1; i++ {
		outslice[i] = inslice[offset+i*sizeDim2]
	}
	return outslice
}

func to3by3arr(inslice []float64, offset int) [3][3]float64 {
	//returns a 3 x 3 array extracted from a 1-d slice that is a representation
	//of an array with shape (len(inslice)/9, n, n)
	var arr [3][3]float64
	for i := 0; i < 3; i++ {
		start := offset*9 + i*3
		copy(arr[i][:], inslice[start:start+3])
	}
	return arr
}

func to6by6arr(inslice []float64) [6][6]float64 {
	//returns a 6 x 6 array extracted from a 1-d slice of len 36
	var arr [6][6]float64
	for i := 0; i < 6; i++ {
		copy(arr[i][:], inslice[i*6:i*6+6])
	}
	return arr
}

func toDateTime(secondsSinceEpoch float64) string {
	dt := time.Duration(secondsSinceEpoch) * time.Second
	start := time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC)
	datetime := start.Add(dt)
	out := datetime.Format(time.RFC3339)
	return out
}

//GetRecords from Level1a file
func GetRecords(fname string) Records {

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
	return records
}

//WriteRecords write records to json file
func WriteRecords(records Records, outputfile string) error {
	outdata, err := json.MarshalIndent(records, "", "    ")
	if err != nil {
		log.Fatalln(err)
		return err
	}
	err = ioutil.WriteFile(outputfile, outdata, 0644)
	if err != nil {
		log.Fatalln(err)
		return err
	}
	return err
}
