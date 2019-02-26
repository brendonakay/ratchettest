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
	"database/sql"
	"github.com/dailyburn/ratchet"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/processors"
	//_ "github.com/go-sql-driver/mysql"
	"encoding/csv"
	"ratchettest/packages"
)

// I have no idea what I'm doing :]
func main() {
	// inputDB := setupDB("mysql", "root:@tcp(127.0.0.1:3306)/srcDB")
	inputCSV := setupCSV("people.csv")
	// Not sure if this is a better approach?
	//inputCsvReader := csv.NewReader(bufio.NewReader(csvFile))
	// extractDP := processors.NewSQLReader(inputDB, mypkg.Query(5)) //TODO: rewrite to extractCSV
	extractCSV := processors.NewFileReader(inputCSV)

	// TODO: Rewrite to CSVTransformaer
	transformDP := mypkg.NewMyTransformer()

	// TODO: Do the same as above with writing out to CSV
	outputDB := setupDB("mysql", "root@tcp(127.0.0.1:3306)/dstDB")
	outputTable := "users2"
	loadDP := processors.NewSQLWriter(outputDB, outputTable)

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
// TODO: return type might be not right
func setupCSV(file) *FileReader {
	csvFile, err := os.Open(file)
	if err != nil {
		logger.Error(err)
		panic(err)
	}
	return csvFile
}
