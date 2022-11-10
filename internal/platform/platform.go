package platform

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"gonum.org/v1/hdf5"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

const epochGPSCorrection = -18 // Seconds

var dateEpochGPS = time.Date(1980, 1, 6, 0, 0, epochGPSCorrection, 0, time.UTC)

var schema = `message schema {
	required int64 time (TIMESTAMP(MILLIS, true));
	optional float eciMpduMainBusVoltage;
	optional float ecoUnitPower_heatStr;
	optional float ecoUnitPower_plMain;
	optional float ecoUnitPower_plSafe;
	optional int32 scoCurrentScMode (UINT_8);
	optional float tcoTemp_pl;
	optional float tcoTemp_sa1;
	optional float tcoTemp_sa2;
	optional float tcoTemp_sc10_pzPanel;
	optional float tcoTemp_str;
	optional group afsAttitudeState (LIST) {
		repeated group list {
			required double element;
		}
	}
	optional group afsAttitudeUncertainty (LIST) {
		repeated group list {
			required group element (LIST) {
				repeated group list {
					required double element;
				}
			}
		}
	}
	optional group afsRateUncertainty (LIST) {
		repeated group list {
			required group element (LIST) {
				repeated group list {
					required double element;
				}
			}
		}
	}
	optional group afsSpacecraftRate (LIST) {
		repeated group list {
			required double element;
		}
	}
	optional group afsTangentPoint (LIST) {
		repeated group list {
			required double element;
		}
	}
	optional group acsGnssStateJ2000 (LIST) {
		repeated group list {
			required double element;
		}
	}
	optional group acsNavigationUncertainty (LIST) {
		repeated group list {
			required group element (LIST) {
				repeated group list {
					required double element;
				}
			}
		}
	}
	optional int32 acoOnGnssPropagationTime (UINT_8);
	optional float acoOnGnssStateEcef_vx;
	optional float acoOnGnssStateEcef_vy;
	optional float acoOnGnssStateEcef_vz;
	optional float acoOnGnssStateEcef_x;
	optional float acoOnGnssStateEcef_y;
	optional float acoOnGnssStateEcef_z;
	optional double acoOnGnssStateTime;
}`

// PowerRecord for variables in group: "HK_ecPowOps_1"
type PowerRecord struct {
	Time           time.Time `parquet:"time"`
	MainBusVoltage float32 `parquet:"eciMpduMainBusVoltage"`
	PowerheatStr   float32 `parquet:"ecoUnitPower_heatStr"`
	PowerplMain    float32 `parquet:"ecoUnitPower_plMain"`
	PowerplSafe    float32 `parquet:"ecoUnitPower_plSafe"`
}

// CurrentRecord for variables in group: "HK_scSysOps_1"
type CurrentRecord struct {
	Time time.Time `parquet:"time"`
	Mode uint8 `parquet:"scoCurrentScMode"`
}

// TemperatureRecord for variables in group: "HK_tcThermEssential"
type TemperatureRecord struct {
	Time            time.Time `parquet:"time"`
	Temppl          float32 `parquet:"tcoTemp_pl"`
	Tempsa1         float32 `parquet:"tcoTemp_sa1"`
	Tempsa2         float32 `parquet:"tcoTemp_sa2"`
	Tempsc10pzPanel float32 `parquet:"tcoTemp_sc10_pzPanel"`
	Tempstr         float32 `parquet:"tcoTemp_str"`
}

// AttitudeRecord for variables in group: "PreciseAttitudeEstimation"
type AttitudeRecord struct {
	Time                time.Time `parquet:"time"`
	AttitudeState       [4]float64    `parquet:"afsAttitudeState"`
	AttitudeUncertainty [3][3]float64 `parquet:"afsAttitudeUncertainty"`
	RateUncertainty     [3][3]float64 `parquet:"afsRateUncertainty"`
	SpacecraftRate      [3]float64    `parquet:"afsSpacecraftRate"`
	TangentPoint        [3]float64    `parquet:"afsTangentPoint"`
}

// OrbitRecord for variables in group: "PreciseOrbitEstimation"
type OrbitRecord struct {
	Time           time.Time `parquet:"time"`
	GnssStateJ2000 [6]float64    `parquet:"acsGnssStateJ2000"`
	Uncertainty    [6][6]float64 `parquet:"acsNavigationUncertainty"`
}

// GnssRecord for variables in group: "TM_acGnssOps"
type GnssRecord struct {
	Time            time.Time `parquet:"time"`
	PropagationTime uint8   `parquet:"acoOnGnssPropagationTime"`
	StateEcefVX     float32 `parquet:"acoOnGnssStateEcef_vx"`
	StateEcefVY     float32 `parquet:"acoOnGnssStateEcef_vy"`
	StateEcefVZ     float32 `parquet:"acoOnGnssStateEcef_vz"`
	StateEcefX      float32 `parquet:"acoOnGnssStateEcef_x"`
	StateEcefY      float32 `parquet:"acoOnGnssStateEcef_y"`
	StateEcefZ      float32 `parquet:"acoOnGnssStateEcef_z"`
	StateTime       float64 `parquet:"acoOnGnssStateTime"`
}

//L1aWrite interface
type L1aWrite interface {
	Write(filename string) error
}

// Records collection of records
type Records struct {
	PowerRecords       []PowerRecord       `parquet:"HK_ecPowOps_1"`
	CurrentRecords     []CurrentRecord     `parquet:"HK_scSysOps_1"`
	TemperatureRecords []TemperatureRecord `parquet:"HK_tcThermEssential"`
	AttitudeRecords    []AttitudeRecord    `parquet:"PreciseAttitudeEstimation"`
	OrbitRecords       []OrbitRecord       `parquet:"PreciseOrbitEstimation"`
	GnssRecords        []GnssRecord        `parquet:"TM_acGnssOps"`
}

//Write records to file
func (r Records) Write(outputfile string) error {
	schemaDef, err := parquetschema.ParseSchemaDefinition(schema)
	if err != nil {
		return err
	}

	fw, err := floor.NewFileWriter(
		outputfile,
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		return err
	}

	for _, rec := range r.PowerRecords {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	for _, rec := range r.CurrentRecords {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	for _, rec := range r.TemperatureRecords {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	for _, rec := range r.AttitudeRecords {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	for _, rec := range r.OrbitRecords {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	for _, rec := range r.GnssRecords {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}

	if err := fw.Close(); err != nil {
		return err
	}

	return err
}

func getDataset(
	filename string, groupname string, datasetname string) (
	*hdf5.Dataset, *hdf5.Group, *hdf5.File) {

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
	return dataset, group, f
}

func getDatasetFloat64(
	filename string, groupname string, datasetname string) []float64 {

	dataset, group, f := getDataset(filename, groupname, datasetname)
	defer dataset.Close()
	defer group.Close()
	defer f.Close()
	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]float64, npoints)
	err := dataset.Read(&data)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname, "data")
	}
	return data
}

func getDatasetFloat32(
	filename string, groupname string, datasetname string) []float32 {

	dataset, group, f := getDataset(filename, groupname, datasetname)
	defer dataset.Close()
	defer group.Close()
	defer f.Close()
	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]float32, npoints)
	err := dataset.Read(&data)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname, "data")
	}
	return data
}

func getDatasetUint8(
	filename string, groupname string, datasetname string) []uint8 {

	dataset, group, f := getDataset(filename, groupname, datasetname)
	defer dataset.Close()
	defer group.Close()
	defer f.Close()
	npoints := dataset.Space().SimpleExtentNPoints()
	data := make([]uint8, npoints)
	err := dataset.Read(&data)
	if err != nil {
		log.Fatalln(err, filename, groupname, datasetname, "data")
	}
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
			Time:           toDateTime(time[i]),
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
			Time: toDateTime(time[i]),
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
			Time:            toDateTime(time[i]),
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
			Time:            toDateTime(time[i]),
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
			Time:        toDateTime(time[i]),
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
		record := AttitudeRecord{Time: toDateTime(time[i])}
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
	//for i := 0; i < sizeDim1; i++ {
	for i := range outslice {
		outslice[i] = inslice[offset+i*sizeDim2]
	}
	return outslice
}

func to3by3arr(inslice []float64, offset int) [3][3]float64 {
	//returns a 3 x 3 array extracted from a 1-d slice that is a representation
	//of an array with shape (len(inslice)/9, n, n)
	var arr [3][3]float64
	for i := range arr {
		start := offset*9 + i*3
		copy(arr[i][:], inslice[start:start+3])
	}
	return arr
}

func to6by6arr(inslice []float64) [6][6]float64 {
	//returns a 6 x 6 array extracted from a 1-d slice of len 36
	var arr [6][6]float64
	for i := range arr {
		copy(arr[i][:], inslice[i*6:i*6+6])
	}
	return arr
}

func toDateTime(secondsSinceEpoch float64) time.Time {
	dt := time.Duration(secondsSinceEpoch*1e9) * time.Nanosecond
	datetime := dateEpochGPS.Add(dt)
	return datetime
}

//GetFilepath manipulates filename
func GetFilepath(inputFile string, outputDirectory string) string {
	outputFile := path.Join(
		outputDirectory,
		strings.TrimSuffix(
			path.Base(inputFile), filepath.Ext(inputFile),
		)+".parquet",
	)
	return outputFile
}

//GetRecords from Level1a hdf file
func GetRecords(fname string) L1aWrite {
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

func getRecordsFromJSONFile(filename string) Records {
	jsonFile, err := os.Open(filename)
	defer jsonFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Fatal(err)
	}
	var records Records
	json.Unmarshal(byteValue, &records)
	return records
}
