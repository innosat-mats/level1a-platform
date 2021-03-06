package platform

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
	"time"
)

const (
	testfile string = "LEVEL_1A_PLATF_20200120-094217_20200120-102136.hdf5"
)

func Test_toDateTime(t *testing.T) {
	type args struct {
		secondsSinceEpoch float64
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{"0", args{0}, time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC)},
		{"1", args{3601.123}, time.Date(1980, 1, 6, 1, 0, 1, 123000000, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toDateTime(tt.args.secondsSinceEpoch); got != tt.want {
				t.Errorf("toDateTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getDatasetFloat64(t *testing.T) {
	type args struct {
		filename    string
		groupname   string
		datasetname string
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		index int
	}{
		{"0", args{testfile, "root/Level1A/HK_ecPowOps_1", "Time"}, 1.2767113859339905e+09, 0},
		{"1", args{testfile, "root/Level1A/HK_ecPowOps_1", "Time"}, 1.2767113959339905e+09, 1},
		{"2", args{testfile, "root/Level1A/HK_ecPowOps_1", "Time"}, 1.2767535851099854e+09, 4220},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDatasetFloat64(tt.args.filename, tt.args.groupname, tt.args.datasetname); !reflect.DeepEqual(got[tt.index], tt.want) {
				t.Errorf("getDatasetFloat64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getDatasetFloat32(t *testing.T) {
	type args struct {
		filename    string
		groupname   string
		datasetname string
	}
	tests := []struct {
		name  string
		args  args
		want  float32
		index int
	}{
		{"0", args{testfile, "root/Level1A/HK_ecPowOps_1", "eciMpduMainBusVoltage"}, 32.867725, 0},
		{"1", args{testfile, "root/Level1A/HK_ecPowOps_1", "eciMpduMainBusVoltage"}, 32.8726, 1},
		{"2", args{testfile, "root/Level1A/HK_ecPowOps_1", "eciMpduMainBusVoltage"}, 32.965195, 4220},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDatasetFloat32(tt.args.filename, tt.args.groupname, tt.args.datasetname); !reflect.DeepEqual(got[tt.index], tt.want) {
				t.Errorf("getDatasetFloat32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getDatasetUint8(t *testing.T) {
	type args struct {
		filename    string
		groupname   string
		datasetname string
	}
	tests := []struct {
		name  string
		args  args
		want  uint8
		index int
	}{
		{"0", args{testfile, "root/Level1A/HK_scSysOps_1", "scoCurrentScMode"}, 4, 0},
		{"1", args{testfile, "root/Level1A/HK_scSysOps_1", "scoCurrentScMode"}, 4, 1},
		{"2", args{testfile, "root/Level1A/HK_scSysOps_1", "scoCurrentScMode"}, 4, 1406},
		{"3", args{testfile, "root/Level1A/TM_acGnssOps", "acoOnGnssPropagationTime"}, 110, 0},
		{"4", args{testfile, "root/Level1A/TM_acGnssOps", "acoOnGnssPropagationTime"}, 120, 1},
		{"5", args{testfile, "root/Level1A/TM_acGnssOps", "acoOnGnssPropagationTime"}, 0, 4220},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDatasetUint8(tt.args.filename, tt.args.groupname, tt.args.datasetname); !reflect.DeepEqual(got[tt.index], tt.want) {
				t.Errorf("getDatasetUint8() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_getPowerRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		index int
		want  PowerRecord
	}{
		{
			"0",
			args{testfile},
			0,
			PowerRecord{toDateTime(1.2767113859339905e+09), 32.867725, 0, 36.02206, 0},
		},
		{
			"1",
			args{testfile},
			1,
			PowerRecord{toDateTime(1.2767113959339905e+09), 32.8726, 0, 36.027405, 0},
		},
		{
			"2",
			args{testfile},
			4220,
			PowerRecord{toDateTime(1.2767535851099854e+09), 32.965195, 0, 36.34141, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getPowerRecords(tt.args.filename)[tt.index]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getPowerRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCurrentRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		index int
		want  CurrentRecord
	}{
		{
			"0", args{testfile},
			0,
			CurrentRecord{toDateTime(1.2767113959339905e+09), 4},
		},
		{
			"1",
			args{testfile},
			1,
			CurrentRecord{toDateTime(1.2767114259339905e+09), 4},
		},
		{
			"2",
			args{testfile},
			1406,
			CurrentRecord{toDateTime(1.2767535751099854e+09), 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getCurrentRecords(tt.args.filename)[tt.index]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCurrentRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getTemperatureRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		index int
		want  TemperatureRecord
	}{
		{
			"0",
			args{testfile},
			0,
			TemperatureRecord{
				toDateTime(1.2767113959339905e+09), 20.010345, 19.996307, 19.996307, 19.96872, 19.978027,
			},
		},
		{
			"1",
			args{testfile},
			1,
			TemperatureRecord{
				toDateTime(1.2767114559339905e+09), 20.010345, 19.996307, 19.996307, 19.96872, 19.978027,
			},
		},
		{
			"2",
			args{testfile},
			703,
			TemperatureRecord{
				toDateTime(1.2767535751099854e+09), 20.010345, 19.996307, 19.996307, 19.96872, 19.978027,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getTemperatureRecords(tt.args.filename)[tt.index]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTemperatureRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAttitudeRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		index int
		want  AttitudeRecord
	}{
		{
			"0", args{testfile}, 0,
			AttitudeRecord{
				toDateTime(1.276711377343e+09),
				[4]float64{0.9300403510407163, 0.08283207095795848, -0.3567427081245257, 0.02997388291257447},
				[3][3]float64{
					{1.180738526120703e-09, -3.856314966778513e-11, 1.1180929218625675e-10},
					{-3.856314966778637e-11, 1.1071146626587574e-09, 6.72203005201567e-11},
					{1.1180929218625737e-10, 6.72203005201565e-11, 8.048304554655132e-09},
				},
				[3][3]float64{
					{8.848448773147466e-10, -4.711576990950077e-11, 1.2934206260875184e-10},
					{-4.711576990950077e-11, 7.935204621384227e-10, 7.945171207850567e-11},
					{1.29342062608751e-10, 7.945171207850402e-11, 1.6953909217038397e-09},
				},
				[3]float64{1.7240612080470676e-05, 0.000639328995688749, 2.6709850229026457e-05},
				[3]float64{0, 0, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getAttitudeRecords(tt.args.filename)[tt.index]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAttitudeRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getGnssRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		index int
		want  GnssRecord
	}{
		{
			"0",
			args{testfile},
			0,
			GnssRecord{toDateTime(1.2767113859339905e+09), 110, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"1",
			args{testfile},
			100,
			GnssRecord{
				toDateTime(1.2767123851099854e+09), 0, 6913.21, -332.256, -3204.408,
				2.9483498e+06, 981803.06, 6.2385e+06, 1.276712385e+09,
			},
		},
		{
			"2",
			args{testfile},
			4220,
			GnssRecord{
				toDateTime(1.2767535851099854e+09), 0, -3186.8157, 808.53485, -6895.182,
				-6.237176e+06, -1.5812031e+06, 2.6921988e+06, 1.276753585e+09,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getGnssRecords(tt.args.filename)[tt.index]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getGnssRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getOrbitRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		index int
		want  OrbitRecord
	}{
		{
			"0", args{testfile}, 0,
			OrbitRecord{
				toDateTime(0),
				[6]float64{0, 0, 0, 0, 0, 0},
				[6][6]float64{
					{1.30100331462404, 0, 0.0742053222749471, 0.0256080096618077, 0, 0.000154822624482059},
					{0, 1.29845451815173, 0, 0, 0.0254768662546276, 0},
					{0.0742053222749471, 0, 4.62980895789729, 0.00585451428534218, 0, 0.0868037807308504},
					{0.0256080096618077, 0, 0.00585451428534218, 0.00111208207146853, 0, 0.00012629439046461},
					{0, 0.0254768662546276, 0, 0, 0.00110191261248683, 0},
					{0.000154822624482059, 0, 0.0868037807308504, 0.00012629439046461, 0, 0.00392052956040496},
				},
			},
		},
		{
			"1", args{testfile}, 4220,
			OrbitRecord{
				toDateTime(1.276753585e+09),
				[6]float64{
					-6.323881153384321e+06, -1.159463796322383e+06, 2.7045646212289413e+06,
					-3223.4601646163687, 1481.9640123949955, -6888.881321980694,
				},
				[6][6]float64{
					{1.30100331462404, 0, 0.0742053222749471, 0.0256080096618077, 0, 0.000154822624482059},
					{0, 1.29845451815173, 0, 0, 0.0254768662546276, 0},
					{0.0742053222749471, 0, 4.62980895789729, 0.00585451428534218, 0, 0.0868037807308504},
					{0.0256080096618077, 0, 0.00585451428534218, 0.00111208207146853, 0, 0.00012629439046461},
					{0, 0.0254768662546276, 0, 0, 0.00110191261248683, 0},
					{0.000154822624482059, 0, 0.0868037807308504, 0.00012629439046461, 0, 0.00392052956040496},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getOrbitRecords(tt.args.filename)[tt.index]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getOrbitRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func makeRange(min, max int) []float64 {
	a := make([]float64, max-min+1)
	minFloat := float64(min)
	for i := range a {
		f := float64(i)
		a[i] = minFloat + f
	}
	return a
}

func Test_to1DSlice(t *testing.T) {
	type args struct {
		inslice  []float64
		sizeDim1 int
		sizeDim2 int
		offset   int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		index int
	}{
		{"0", args{makeRange(0, 100), 3, 10, 0}, 0, 0},
		{"1", args{makeRange(0, 100), 3, 10, 0}, 10, 1},
		{"2", args{makeRange(0, 100), 3, 10, 0}, 20, 2},
		{"3", args{makeRange(0, 100), 3, 10, 1}, 1, 0},
		{"4", args{makeRange(0, 100), 4, 10, 1}, 1, 0},
		{"5", args{makeRange(0, 100), 4, 10, 1}, 31, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := to1DSlice(tt.args.inslice, tt.args.sizeDim1, tt.args.sizeDim2, tt.args.offset); !reflect.DeepEqual(got[tt.index], tt.want) {
				t.Errorf("to1DSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_to3by3arr(t *testing.T) {
	type args struct {
		inslice []float64
		offset  int
	}
	tests := []struct {
		name   string
		args   args
		want   float64
		index1 int
		index2 int
	}{
		{"0", args{makeRange(0, 100), 0}, 0, 0, 0},
		{"1", args{makeRange(0, 100), 0}, 1, 0, 1},
		{"2", args{makeRange(0, 100), 0}, 3, 1, 0},
		{"3", args{makeRange(0, 100), 1}, 9, 0, 0},
		{"4", args{makeRange(0, 100), 1}, 13, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := to3by3arr(tt.args.inslice, tt.args.offset); !reflect.DeepEqual(got[tt.index1][tt.index2], tt.want) {
				t.Errorf("to3by3arr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_to6by6arr(t *testing.T) {
	type args struct {
		inslice []float64
	}
	tests := []struct {
		name   string
		args   args
		want   float64
		index1 int
		index2 int
	}{
		{"0", args{makeRange(0, 35)}, 0, 0, 0},
		{"1", args{makeRange(0, 35)}, 1, 0, 1},
		{"2", args{makeRange(0, 35)}, 6, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := to6by6arr(tt.args.inslice); !reflect.DeepEqual(got[tt.index1][tt.index2], tt.want) {
				t.Errorf("to6by6arr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFilepath(t *testing.T) {
	type args struct {
		inputFile       string
		outputDirectory string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"0", args{"/test/mats.h5", "/out"}, "/out/mats.json"},
		{"1", args{"/test/mats", "/out"}, "/out/mats.json"},
		{"2", args{"mats.h5", "/out"}, "/out/mats.json"},
		{"3", args{"johnny.h5", "/tmp/out/"}, "/tmp/out/johnny.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFilepath(tt.args.inputFile, tt.args.outputDirectory); got != tt.want {
				t.Errorf("GetFilepath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecords_Write(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "mats-testing")
	if err != nil {
		log.Fatal(err)
	}
	outputFile := path.Join(dir, "test.json")
	defer os.RemoveAll(dir)
	type fields struct {
		PowerRecords       []PowerRecord
		CurrentRecords     []CurrentRecord
		TemperatureRecords []TemperatureRecord
		AttitudeRecords    []AttitudeRecord
		OrbitRecords       []OrbitRecord
		GnssRecords        []GnssRecord
	}
	type args struct {
		outputfile string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"0",
			fields{
				[]PowerRecord{PowerRecord{Time: toDateTime(0)}},
				[]CurrentRecord{CurrentRecord{Time: toDateTime(1)}},
				[]TemperatureRecord{TemperatureRecord{Time: toDateTime(2)}},
				[]AttitudeRecord{AttitudeRecord{Time: toDateTime(3)}},
				[]OrbitRecord{OrbitRecord{Time: toDateTime(4)}},
				[]GnssRecord{GnssRecord{Time: toDateTime(5)}},
			},
			args{outputFile},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Records{
				PowerRecords:       tt.fields.PowerRecords,
				CurrentRecords:     tt.fields.CurrentRecords,
				TemperatureRecords: tt.fields.TemperatureRecords,
				AttitudeRecords:    tt.fields.AttitudeRecords,
				OrbitRecords:       tt.fields.OrbitRecords,
				GnssRecords:        tt.fields.GnssRecords,
			}
			if err := r.Write(tt.args.outputfile); (err != nil) != tt.wantErr {
				t.Errorf("Records.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			//read generated file and check that time is ok
			records := getRecordsFromJSONFile(tt.args.outputfile)
			if records.PowerRecords[0].Time != toDateTime(0) {
				t.Errorf("unmarshaled power records not consistent to input records")
			}
			if records.CurrentRecords[0].Time != toDateTime(1) {
				t.Errorf("unmarshaled current records not consistent to input records")
			}
			if records.TemperatureRecords[0].Time != toDateTime(2) {
				t.Errorf("unmarshaled temperture records not consistent to input records")
			}
			if records.AttitudeRecords[0].Time != toDateTime(3) {
				t.Errorf("unmarshaled attitude records not consistent to input records")
			}
			if records.OrbitRecords[0].Time != toDateTime(4) {
				t.Errorf("unmarshaled orbit records not consistent to input records")
			}
			if records.GnssRecords[0].Time != toDateTime(5) {
				t.Errorf("unmarshaled gnss records not consistent to input records")
			}

		})
	}
}

func TestRecords_WriteFail(t *testing.T) {
	type fields struct {
		PowerRecords []PowerRecord
	}
	type args struct {
		outputfile string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test fails and returns error",
			fields{
				[]PowerRecord{PowerRecord{Time: toDateTime(0)}},
			},
			args{"/non/ok/directory/for/file.json"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Records{
				PowerRecords: tt.fields.PowerRecords,
			}
			if err := r.Write(tt.args.outputfile); (err != nil) != tt.wantErr {
				t.Errorf("Records.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetRecords(t *testing.T) {
	type args struct {
		fname string
	}
	tests := []struct {
		name string
		args args
		want L1aWrite
	}{
		{"Test underlying output struct is Records", args{testfile}, Records{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetRecords(tt.args.fname); !reflect.DeepEqual(reflect.TypeOf(got), reflect.TypeOf(tt.want)) {
				t.Errorf("GetRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}
