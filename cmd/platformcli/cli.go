package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/innosat-mats/level1a-platform/internal/platform"
)

var outputDirectory *string
var stdout *bool

func processFiles(
	inputFiles []string, stdout bool, outputDirectory string) error {

	for _, inputFile := range inputFiles {
		records := platform.GetRecords(inputFile)
		if stdout {
			fmt.Println(records)
		}
		if outputDirectory != "" {
			outputFile := path.Join(
				outputDirectory,
				strings.TrimSuffix(
					path.Base(inputFile), filepath.Ext(inputFile),
				)+".json",
			)
			err := platform.WriteRecords(records, outputFile)
			if err != nil {
				log.Fatalln(err)
				return err
			}
		}
	}
	return nil
}

//myUsage replaces default usage since it doesn't include information on non-flags
func myUsage() {
	fmt.Println("Extracts information from Innosat-MATS Level1A plaform files")
	fmt.Println()
	fmt.Printf("Usage: %s [OPTIONS] level1a-file ...\n", os.Args[0])
	fmt.Println()
	flag.PrintDefaults()
}

func init() {
	outputDirectory = flag.String("output", "", "Directory to place timeseries data files")
	stdout = flag.Bool("stdout", false, "Output to standard out instead of to disk (only timeseries)\n(Default: false)")
	flag.Usage = myUsage
}

func main() {
	flag.Parse()
	inputFiles := flag.Args()
	if len(inputFiles) == 0 {
		flag.Usage()
		fmt.Println("\nNo level1a-files supplied")
		os.Exit(1)
	}
	if *outputDirectory == "" && !*stdout {
		flag.Usage()
		fmt.Println("\nExpected an output directory")
		os.Exit(1)
	}

	err := processFiles(inputFiles, *stdout, *outputDirectory)
	if err != nil {
		log.Fatal(err)
	}
}
