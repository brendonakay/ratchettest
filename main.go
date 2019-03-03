/*
Tutorial: http://rkulla.blogspot.com/2016/01/data-pipeline-and-etl-tasks-in-go-using.html
TP stands for "Data Pipeline"
TODO:
	- Refactor mysql input & processors reader to csv
		- Rewrite setupDB to setupCSV
		- ** Rewrite SQL reader to FileReader
*/
package main

import (
	"os"
	"io"
	//"bufio"
	"github.com/dailyburn/ratchet"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/processors"
	//"encoding/csv"
	"ratchettest/packages"
)

// I have no idea what I'm doing :]
func main() {
	csvFile := setupCSV("people.csv")
	//	writer := csv.NewWriter(bufio.NewWriter(csvFile))
	//extractCSV := processors.NewFileReader(inputCSV)
	extractDP := processors.NewFileReader("people.csv")

	// TODO: Rewrite to CSVTransformaer
	transformDP := mypkg.NewMyTransformer()

	loadDP := processors.NewCSVWriter(csvFile)

	// TODO: refactior da pipelane
	pipeline := ratchet.NewPipeline(extractDP, transformDP, loadDP)
	pipeline.Name = "My Pipeline"

	err := <-pipeline.Run()
	if err != nil {
		logger.ErrorWithoutTrace(pipeline.Name, ":", err)
		logger.ErrorWithoutTrace(pipeline.Stats())
	} else {
		logger.Info(pipeline.Name, ": Completed successfully.")
	}
}

// Open the CSV file. CSV file must be in same directory as program.
// TODO: return type might be not right | maybe now tho?
func setupCSV(file string) io.Writer {
	csvFile, err := os.Open(file)
	if err != nil {
		logger.Error(err)
		panic(err)
	}
	return csvFile
}
