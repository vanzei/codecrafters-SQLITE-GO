package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type SQLiteSchemaRow struct {
	_type    string // _type since type is a reserved keyword
	name     string
	tblName  string
	rootPage int
	sql      string
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo", ".tables":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		// Read page size from bytes 16-17 of the database header
		header := make([]byte, 100)
		_, err = databaseFile.ReadAt(header, 0)
		if err != nil {
			log.Fatal(err)
		}
		pageSize := int(header[16])<<8 | int(header[17])

		_, _ = databaseFile.Seek(100, io.SeekStart) // Skip the database header

		pageHeader := parsePageHeader(databaseFile)

		cellPointers := make([]uint16, pageHeader.numCells)
		for i := 0; i < int(pageHeader.numCells); i++ {
			cellPointers[i] = parseUInt16(databaseFile)
		}

		var sqliteSchemaRows []SQLiteSchemaRow
		var tableNames []string

		for _, cellPointer := range cellPointers {
			_, _ = databaseFile.Seek(int64(cellPointer), io.SeekStart)
			parseVarint(databaseFile) // number of bytes in payload
			parseVarint(databaseFile) // rowid
			record := parseRecord(databaseFile, 5)

			sqliteSchemaRows = append(sqliteSchemaRows, SQLiteSchemaRow{
				_type:    string(record.values[0].([]byte)),
				name:     string(record.values[1].([]byte)),
				tblName:  string(record.values[2].([]byte)),
				rootPage: int(record.values[3].(uint8)),
				sql:      string(record.values[4].([]byte)),
			})

			tableNames = append(tableNames, string(record.values[2].([]byte)))
		}
		fmt.Println("database page size: ", pageSize)
		//fmt.Println("Logs from your program will appear here!")
		if command == ".dbinfo" {
			fmt.Printf("number of tables: %v\n", len(sqliteSchemaRows))
		} else {
			fmt.Println(strings.Join(tableNames, " "))
		}
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

// Alias for your parserHeader
func parsePageHeader(databaseFile *os.File) PageHearder {
	return parserHeader(databaseFile)
}

// Alias for your parserRecord
func parseRecord(stream io.Reader, valuesCount int) Record {
	return parserRecord(stream, valuesCount)
}
