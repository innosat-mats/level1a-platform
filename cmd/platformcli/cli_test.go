package main

import (
	"errors"
	"testing"
	"time"

	"github.com/innosat-mats/level1a-platform/internal/platform"
)

type fakeRecord struct {
	Time time.Time
}

//Write fake records to file
func (r fakeRecord) Write(outputfile string) error {
	if outputfile == "/fail/test.json" {
		return errors.New("fail")
	}
	return nil
}

func recordsGetter(fname string) platform.L1aWrite {
	record := fakeRecord{Time: time.Now()}
	return record
}

func Test_processFiles(t *testing.T) {
	type args struct {
		recordsGetter   getter
		inputFiles      []string
		stdout          bool
		outputDirectory string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"test stdout true does not write to disk",
			args{recordsGetter, []string{"test.h5"}, true, "/fail/"}, false},
		{
			"test write to disk fail returns error",
			args{recordsGetter, []string{"test.h5"}, false, "/fail/"}, true},
		{
			"test write to disk ok",
			args{recordsGetter, []string{"test.h5"}, true, "/ok/"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := processFiles(tt.args.recordsGetter, tt.args.inputFiles, tt.args.stdout, tt.args.outputDirectory); (err != nil) != tt.wantErr {
				t.Errorf("processFiles() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
