/*
Tutorial: http://rkulla.blogspot.com/2016/01/data-pipeline-and-etl-tasks-in-go-using.html
TP stands for "Data Pipeline"
TODO:

	- Refactor mysql input & processors reader to csv

		o DEBUG unmarshalling/invalid characters on transform
			- if you look at the library, it’s going to parse the contents of the line and try to convert that line into a struct. your code is not using the header row to define the attribute name, and go is choking on unmarshalling `4` into a valid struct
			sql reader (which calls util.GetDataFromSQLQuery) creates json from headers and rows (see https://github.com/dailyburn/ratchet/blob/develop/util/sql.go)
			you will need to do something similar in order to unmarshal that row into a struct
			- take a look at the util CSV tools for some clues: 
			https://github.com/dailyburn/ratchet/blob/develop/util/csv_writer.go 
			and https://github.com/dailyburn/ratchet/blob/develop/util/csv.go

		o Implement NewCsvReader and CsvReader
			- Look at NewSqlReader
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
	logger.LogLevel = logger.LevelDebug
	//csvFile := setupCSV("peopleTransformed.csv")
	//extractDP := processors.NewFileReader("people.csv")
	csvFile, _ := os.Open("people.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))
			

	// TODO: Rewrite to CSVTransformaer
	transformDP := mypkg.NewMyTransformer()

	loadDP := processors.NewCSVWriter(csvFile)

	// TODO: refactior da pipelane
	pipeline := ratchet.NewPipeline(extractDP, transformDP, loadDP)
	pipeline.Name = "My Pipeline"
	pipeline.PrintData = true

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
