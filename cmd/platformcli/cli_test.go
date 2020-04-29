package main

import (
	"errors"
	"testing"

	"github.com/innosat-mats/level1a-platform/internal/platform"
)

func recordsGetter(fname string) platform.Records {
	currentRecords := platform.CurrentRecord{Time: 0, Mode: 0}
	return platform.Records{
		CurrentRecords: []platform.CurrentRecord{currentRecords},
	}
}

func recordsWriterOk(records platform.Records, outputfile string) error {
	return nil
}

func recordsWriterError(records platform.Records, outputfile string) error {
	return errors.New("fail")
}

func Test_processFiles(t *testing.T) {
	type args struct {
		recordsGetter   platform.RecordsGetter
		recordsWriter   platform.RecordsWriter
		inputFiles      []string
		stdout          bool
		outputDirectory string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"testwritefilesfail", args{recordsGetter, recordsWriterError, []string{"test.h5"}, true, "/tmp"}, true},
		{"testnotfail_nooutdir", args{recordsGetter, recordsWriterError, []string{"test.h5"}, true, ""}, false},
		{"testwritefilesnofail", args{recordsGetter, recordsWriterOk, []string{"test.h5"}, true, "/tmp"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := processFiles(tt.args.recordsGetter, tt.args.recordsWriter, tt.args.inputFiles, tt.args.stdout, tt.args.outputDirectory); (err != nil) != tt.wantErr {
				t.Errorf("processFiles() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
