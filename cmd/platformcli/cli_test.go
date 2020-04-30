package main

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/innosat-mats/level1a-platform/internal/platform"
)

func recordsGetter(fname string) platform.Records {
	currentRecords := platform.CurrentRecord{
		Time: time.Now(),
		Mode: 0,
	}
	return platform.Records{
		CurrentRecords: []platform.CurrentRecord{currentRecords},
	}
}

func Test_processFiles(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "mats-testing")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
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
			args{recordsGetter, []string{"test.h5"}, true, dir}, false},
		{
			"test write to disk fail returns error",
			args{recordsGetter, []string{"test.h5"}, false, "non-existing-dir"}, true},
		{
			"test write to disk ok",
			args{recordsGetter, []string{"test.h5"}, true, dir}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := processFiles(tt.args.recordsGetter, tt.args.inputFiles, tt.args.stdout, tt.args.outputDirectory); (err != nil) != tt.wantErr {
				t.Errorf("processFiles() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
