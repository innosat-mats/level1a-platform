package main

import (
	"reflect"
	"testing"
)

func Test_toDateTime(t *testing.T) {
	type args struct {
		secondsSinceEpoch float64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"bengt",
			args{0},
			"1980-01-06T00:00:00Z",
		},
		{
			"bengt",
			args{1},
			"1980-01-06T00:00:01Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toDateTime(tt.args.secondsSinceEpoch); got != tt.want {
				t.Errorf("toDateTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAttitudeRecords(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			"bengt",
			args{"LEVEL_1A_PLATF_20200120-094217_20200120-102136.hdf5"},
			1.276711377343e+09,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getAttitudeRecords(tt.args.filename)[0].Time; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAttitudeRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}
