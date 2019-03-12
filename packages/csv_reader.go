package processors

import (
	"errors"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/util"
)

// Copied from github.com/dailyburn/ratchet/processors/sql_reader.go
//
// SQLReader runs the given SQL and passes the resulting data
// to the next stage of processing.
//
// It can operate in 2 modes:
// 1) Static - runs the given SQL query and ignores any received data.
// 2) Dynamic - generates a SQL query for each data payload it receives.
//
// The dynamic SQL generation is implemented by passing in a "sqlGenerator"
// function to NewDynamicSQLReader. This allows you to write whatever code is
// needed to generate SQL based upon data flowing through the pipeline.
type CsvReader struct {
	readCsv           string 
	csvGenerator      func(data.JSON) (string, error)
	BatchSize         int
	StructDestination interface{}
	ConcurrencyLevel  int // See ConcurrentDataProcessor
}

type dataErr struct {
	Error string
}

// NewCsvReader returns a new CsvReader operating in static mode.
func NewCsvReader(csv string) *CsvReader {
	return &CsvReader{readCsv: dbConn, BatchSize: 1000}
}

// NewDynamicCsvReader returns a new CsvReader operating in dynamic mode.
//func NewDynamicCsvReader(dbConn *sql.DB, csvGenerator func(data.JSON) (string, error)) *CsvReader {
//	return &CsvReader{readCsv: dbConn, csvGenerator: csvGenerator, BatchSize: 1000}
//}

// ProcessData - see interface for documentation.
func (s *CsvReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	s.ForEachQueryData(d, killChan, func(d data.JSON) {
		outputChan <- d
	})
}

// ForEachQueryData handles generating the SQL (in case of dynamic mode),
// running the query and retrieving the data in data.JSON format, and then
// passing the results back witih the function call to forEach.
func (s *CsvReader) ForEachQueryData(d data.JSON, killChan chan error, forEach func(d data.JSON)) {
	sql := ""
	var err error
	if s.query == "" && s.csvGenerator != nil {
		sql, err = s.csvGenerator(d)
		util.KillPipelineIfErr(err, killChan)
	} else if s.query != "" {
		sql = s.query
	} else {
		killChan <- errors.New("CsvReader: must have either static query or csvGenerator func")
	}

	logger.Debug("CsvReader: Running - ", sql)
	// See sql.go
	dataChan, err := util.GetDataFromCsv(s.readCsv, sql, s.BatchSize, s.StructDestination)
	util.KillPipelineIfErr(err, killChan)

	for d := range dataChan {
		// First check if an error was returned back from the SQL processing
		// helper, then if not call forEach with the received data.
		var derr dataErr
		if err := data.ParseJSONSilent(d, &derr); err == nil {
			util.KillPipelineIfErr(errors.New(derr.Error), killChan)
		} else {
			forEach(d)
		}
	}
}

// Finish - see interface for documentation.
func (s *CsvReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *CsvReader) String() string {
	return "CsvReader"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *CsvReader) Concurrency() int {
	return s.ConcurrencyLevel
}
