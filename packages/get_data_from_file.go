package mypkg

import (
	"bufio"
	"fmt"
	"io"
    "os"
	"strings"
	"unicode"
	"unicode/utf8"
	"github.com/dailyburn/ratchet/data"
)

// TODO: Move these toa models.go
type Person struct {
    Firstname string   `json:"firstname"`
    Lastname  string   `json:"lastname"`
    Address   *Address `json:"address,omitempty"`
}

type Address struct {
    City  string `json:"city"`
    State string `json:"state"`
}

// Copied from github.com/dailyburn/ratchet/util/sql.go
//
// Refactor this but for CSV
//
// GetDataFromSQLQuery is a util function that, given a properly intialized sql.DB
// and a valid SQL query, will handle executing the query and getting back data.JSON
// objects. This function is asynch, and data.JSON should be received on the return
// data channel. If there was a problem setting up the query, then an error will also be
// returned immediately. It is also possible for errors to occur during execution as data
// is retrieved from the query. If this happens, the object returned will be a JSON
// object in the form of {"Error": "description"}.
func GetDataFromCsv(csv string, batchSize int) (chan data.JSON, error) {
	//stmt, err := db.Prepare(query)
    csvFile, err := os.Open(csv)
	if err != nil {
		return nil, err
	}
	defer os.Close()

    reader := csv.NewReader(bufio.NewReader(csvFile))

	//rows, err := stmt.Query()
	//if err != nil {
	//	return nil, err
	//}
    //
	//columns, err := rows.Columns()
	//if err != nil {
	//	return nil, err
	//}

	dataChan := make(chan data.JSON)

    go scanDataGeneric(csv, batchSize, dataChan)

	return dataChan, nil
}

// Overwrite github.com/dailyburn/ratchet/util.ScanDataGeneric
func scanDataGeneric(rows *Reader, columns int, batchSize int, dataChan chan data.JSON) {
	defer os.Close()

	csvData := []map[string]interface{}{}
	values := make([]interface{}, len(columns))
    // I have no idea what this is used for
	valuePtrs := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		//err := rows.Scan(valuePtrs...)
		//if err != nil {
		//	sendErr(err, dataChan)
		//}

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			// logger.Debug("Value Type for", col, " -> ", reflect.TypeOf(val))
			switch vv := val.(type) {
			case []byte:
				v = string(vv)
			default:
				v = vv
			}
			entry[col] = v
		}
		csvData = append(csvData, entry)

		if batchSize > 0 && len(csvData) >= batchSize {
            // TODO: implement sendCSVData
			sendCsvData(csvData, dataChan)
			csvData = []map[string]interface{}{}
		}
	}
	//if rows.Err() != nil {
	//	sendErr(rows.Err(), dataChan)
	//}

	// Flush remaining csvData
	if len(csvData) > 0 {
		sendCsvData(csvData, dataChan)
	}

	close(dataChan) // signal completion to caller
}

func sendCsvData(csvData []map[string]interface{}, dataChan chan data.JSON) {
	d, err := data.NewJSON(csvData)
	if err != nil {
		sendErr(err, dataChan)
	} else {
		dataChan <- d
	}
}
